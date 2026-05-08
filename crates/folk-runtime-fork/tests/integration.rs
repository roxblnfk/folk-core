//! Integration test — spawns real PHP fork master and forks a child.
//! Requires `php` in PATH with `pcntl`, `sockets`, and `msgpack` extensions.

use std::time::Duration;

use folk_core::runtime::Runtime;
use folk_protocol::RpcMessage;
use folk_runtime_fork::{ForkConfig, ForkRuntime};
use rmpv::Value;

#[tokio::test]
#[ignore = "requires php + pcntl + sockets + msgpack extensions"]
async fn fork_runtime_spawns_worker_via_fork() {
    // Write a PHP fork-master script to a temp file
    let script_file = {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(PHP_FORK_MASTER.as_bytes()).unwrap();
        f
    };

    let config = ForkConfig {
        php: "php".into(),
        script: script_file.path().to_str().unwrap().to_string(),
        boot_timeout: Duration::from_secs(10),
    };

    let runtime = ForkRuntime::new(config)
        .await
        .expect("ForkRuntime::new failed");

    let mut worker = runtime.spawn().await.expect("spawn failed");

    // Await control.ready from the forked child
    let ready = tokio::time::timeout(Duration::from_secs(5), worker.recv_control())
        .await
        .expect("timed out waiting for control.ready")
        .expect("recv_control failed");

    let Some(RpcMessage::Notify { method, .. }) = ready else {
        panic!("expected Notify, got {ready:?}");
    };
    assert_eq!(method, "control.ready");

    // Send echo request
    let request = RpcMessage::request(1, "echo", Value::String("fork-test".into()));
    worker.send_task(request).await.expect("send_task failed");

    let response = tokio::time::timeout(Duration::from_secs(5), worker.recv_task())
        .await
        .expect("timed out waiting for response")
        .expect("recv_task failed")
        .expect("EOF before response");

    match response {
        RpcMessage::Response { msgid, result, .. } => {
            assert_eq!(msgid, 1);
            assert_eq!(result.as_str(), Some("fork-test"));
        },
        other => panic!("unexpected: {other:?}"),
    }

    worker.terminate().await.expect("terminate failed");
}

// PHP fork-master script that:
// 1. Sends control.fork-ready
// 2. Receives 2 FDs via SCM_RIGHTS on task socket
// 3. Reads fork.spawn command on control socket
// 4. Forks, child sends control.ready + echoes one request
// 5. Parent sends fork.spawned with child PID
const PHP_FORK_MASTER: &str = r"<?php
declare(strict_types=1);

if (!extension_loaded('pcntl')) { fwrite(STDERR, 'requires pcntl\n'); exit(1); }
if (!extension_loaded('sockets')) { fwrite(STDERR, 'requires sockets\n'); exit(1); }

$taskFd = (int) getenv('FOLK_TASK_FD');
$controlFd = (int) getenv('FOLK_CONTROL_FD');

$taskSock = socket_import_stream(fopen('php://fd/' . $taskFd, 'r+b'));
$control = fopen('php://fd/' . $controlFd, 'r+b');

function read_frame($fd): ?string {
    $header = fread($fd, 4);
    if ($header === false || strlen($header) < 4) return null;
    $len = unpack('Nlen', $header)['len'];
    $data = '';
    $remaining = $len;
    while ($remaining > 0) {
        $chunk = fread($fd, $remaining);
        if ($chunk === false) return null;
        $data .= $chunk;
        $remaining -= strlen($chunk);
    }
    return $data;
}

function write_frame($fd, string $payload): void {
    fwrite($fd, pack('N', strlen($payload)) . $payload);
    fflush($fd);
}

// Send control.fork-ready
write_frame($control, msgpack_pack([2, 'control.fork-ready', ['pid' => getmypid()]]));

// Install SIGCHLD handler
pcntl_signal(SIGCHLD, function() { while (pcntl_waitpid(-1, $s, WNOHANG) > 0) {} });

// Fork loop (handle one fork command)
pcntl_signal_dispatch();

// Receive 2 FDs via SCM_RIGHTS
$msg = ['iov' => [['iov_base' => '', 'iov_len' => 1]], 'control' => []];
$ret = socket_recvmsg($taskSock, $msg, 0);
if ($ret === false) exit(0);

$receivedFds = [];
foreach ($msg['control'] as $cmsg) {
    if (isset($cmsg['cmsg_level']) && $cmsg['cmsg_level'] === SOL_SOCKET) {
        $receivedFds = $cmsg['cmsg_data'] ?? [];
    }
}

// Read fork.spawn command
$frame = read_frame($control);
if ($frame === null) exit(0);

$childPid = pcntl_fork();
if ($childPid === -1) { exit(1); }

if ($childPid === 0) {
    // CHILD: use received FDs as task/control sockets
    $childTask = fopen('php://fd/' . $receivedFds[0], 'r+b');
    $childCtrl = fopen('php://fd/' . $receivedFds[1], 'r+b');

    // Send control.ready
    write_frame($childCtrl, msgpack_pack([2, 'control.ready', ['pid' => getmypid()]]));

    // Echo one request
    $req = read_frame($childTask);
    if ($req !== null) {
        $decoded = msgpack_unpack($req);
        $response = msgpack_pack([1, $decoded[1], null, $decoded[3]]);
        write_frame($childTask, $response);
    }
    exit(0);
}

// PARENT: send fork.spawned
write_frame($control, msgpack_pack([2, 'fork.spawned', ['pid' => $childPid]]));

// Wait for child
pcntl_waitpid($childPid, $status);
";
