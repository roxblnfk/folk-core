//! Integration test — spawns real PHP process.
//! Requires `php` in PATH with the `msgpack` extension installed.

use std::time::Duration;

use folk_core::runtime::Runtime;
use folk_protocol::RpcMessage;
use folk_runtime_pipe::{PipeConfig, PipeRuntime};
use rmpv::Value;

// Minimal PHP script:
// - Opens FD 3 (task) and FD 4 (control).
// - Sends control.ready on FD 4.
// - Reads one request from FD 3, echoes it back.
const PHP_ECHO_WORKER: &str = r"<?php
declare(strict_types=1);
$task_fd    = fopen('php://fd/' . getenv('FOLK_TASK_FD'), 'r+b');
$control_fd = fopen('php://fd/' . getenv('FOLK_CONTROL_FD'), 'r+b');
if (!$task_fd || !$control_fd) { exit(1); }

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
    $len = strlen($payload);
    fwrite($fd, pack('N', $len) . $payload);
    fflush($fd);
}

// Send control.ready
$ready = msgpack_pack([2, 'control.ready', ['pid' => getmypid()]]);
write_frame($control_fd, $ready);

// Echo one request
$frame = read_frame($task_fd);
if ($frame === null) exit(0);
$msg = msgpack_unpack($frame);
// $msg = [0, msgid, method, params] — Request
$response = msgpack_pack([1, $msg[1], null, $msg[3]]); // Response with same params as result
write_frame($task_fd, $response);
";

#[tokio::test]
#[ignore = "requires php in PATH with msgpack extension"]
async fn pipe_runtime_spawns_php_and_completes_echo() {
    // Write the PHP script to a temp file.
    let script_file = {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(PHP_ECHO_WORKER.as_bytes()).unwrap();
        f
    };

    let config = PipeConfig {
        php: "php".into(),
        script: script_file.path().to_str().unwrap().to_string(),
    };

    let rt = PipeRuntime::new(config);
    let mut worker = rt.spawn().await.expect("spawn failed");

    // Await control.ready with timeout
    let ready = tokio::time::timeout(Duration::from_secs(5), worker.recv_control())
        .await
        .expect("timed out waiting for control.ready")
        .expect("recv_control failed");

    let Some(RpcMessage::Notify { method, .. }) = ready else {
        panic!("expected Notify, got {ready:?}");
    };
    assert_eq!(method, "control.ready");

    // Send an echo request on the task channel
    let request = RpcMessage::request(1, "echo", Value::String("test payload".into()));
    worker.send_task(request).await.expect("send_task failed");

    let response = tokio::time::timeout(Duration::from_secs(5), worker.recv_task())
        .await
        .expect("timed out waiting for response")
        .expect("recv_task failed")
        .expect("EOF before response");

    match response {
        RpcMessage::Response { msgid, result, .. } => {
            assert_eq!(msgid, 1);
            assert_eq!(result.as_str(), Some("test payload"));
        },
        other => panic!("unexpected: {other:?}"),
    }

    // Clean terminate
    worker.terminate().await.expect("terminate failed");
}
