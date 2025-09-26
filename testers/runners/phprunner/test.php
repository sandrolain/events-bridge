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

echo $head . "\n";
// Output as query string
echo http_build_query($data);
