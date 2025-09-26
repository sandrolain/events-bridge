<?php

// Leggi la richiesta HTTP da stdin
$stdin = fopen('php://stdin', 'r');
$input = '';

while (($line = fgets($stdin)) !== false) {
  $input .= $line;
}
fclose($stdin);


// Parse JSON
// Get data after first newline
$pos = strpos($input, "\n");
$head = substr($input, 0, $pos);
$json = ($pos !== false) ? substr($input, $pos + 1) : $input;
$data = json_decode($json, true);


if (json_last_error() !== JSON_ERROR_NONE) {
  echo "Invalid JSON input.\n";
  exit(1);
}

$data['php_runner'] = "Processed by PHP runner";

$body = json_encode($data);

echo "$head\n$body";
exit(0);
