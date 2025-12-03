<?php
/**
 * Events Bridge PHP Handler
 * 
 * This script processes incoming messages from Events Bridge via FastCGI.
 * It reads the request body (JSON), processes it, and returns a response.
 */

// Set response headers
header('Content-Type: application/json');

// Get environment variables passed from Events Bridge
$appEnv = getenv('APP_ENV') ?: 'development';
$appDebug = getenv('APP_DEBUG') === 'true';

// Get request information
$requestMethod = $_SERVER['REQUEST_METHOD'] ?? 'GET';
$contentType = $_SERVER['CONTENT_TYPE'] ?? 'application/octet-stream';
$contentLength = $_SERVER['CONTENT_LENGTH'] ?? 0;

// Read the request body
$rawInput = file_get_contents('php://input');

// Try to decode as JSON
$inputData = json_decode($rawInput, true);
$jsonError = json_last_error();

// Build response
$response = [
    'status' => 'processed',
    'timestamp' => date('c'),
    'environment' => $appEnv,
    'debug' => $appDebug,
    'request' => [
        'method' => $requestMethod,
        'contentType' => $contentType,
        'contentLength' => (int)$contentLength,
    ],
    'input' => [
        'raw_length' => strlen($rawInput),
        'json_valid' => $jsonError === JSON_ERROR_NONE,
        'data' => $inputData,
    ],
    'metadata' => [],
];

// Extract HTTP_ headers (metadata from Events Bridge)
foreach ($_SERVER as $key => $value) {
    if (strpos($key, 'HTTP_') === 0) {
        $headerName = strtolower(str_replace('_', '-', substr($key, 5)));
        $response['metadata'][$headerName] = $value;
    }
}

// Process the input data (example logic)
if ($inputData !== null) {
    // Example: add processing result
    $response['processing'] = [
        'message_received' => $inputData['message'] ?? null,
        'data_keys' => isset($inputData['data']) ? array_keys($inputData['data']) : [],
        'processed_at' => microtime(true),
    ];
}

// Output response
echo json_encode($response, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
