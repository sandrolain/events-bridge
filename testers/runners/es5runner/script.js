// Example ES5 runner script for the ES5Runner plugin.
//
// This script assumes a service named `redis` is injected into the VM
// by the Go runner (see `ES5Runner.Process`). The service exposes a
// `Call` method which proxies to the underlying connectors.Service.
//
// The script performs a SET followed by a GET and stores the results
// in the RunnerMessage metadata so the host can observe them.

(function () {
  // Key/value used for the example
  const key = "es5runner:example:key";
  const value = "hello from es5 runner";

  try {
    // Execute SET command on redis service
    // The Call method signature is: Call(command, arg1, arg2, ...)
    const setRes = redis.Call("SET", key, value);

    // Optionally verify by doing a GET
    const getRes = redis.Call("GET", key);

    // Convert results to string and attach to message metadata.
    // The proxy returns the raw []byte result from the service; it
    // may already be a string depending on the Go<->JS conversion.
    message.AddMetadata("redis.set.result", String(setRes));
    message.AddMetadata("redis.get.result", String(getRes));
  } catch (err) {
    // Record the error on the message metadata so the runner can see it
    message.AddMetadata("es5runner.error", String(err));
    // Re-throw to make the runner treat this as a script failure if desired
    throw err;
  }
})();
