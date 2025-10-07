// Example ES5 runner script for the ES5Runner plugin.
//
// This script assumes a service named `redis` is injected into the VM
// by the Go runner (see `ES5Runner.Process`). The service exposes a
// `Call` method which proxies to the underlying connectors.Service.
//
// The script performs a SET followed by a GET and stores the results
// in the RunnerMessage metadata so the host can observe them.

(function () {
  const data = message.getDataString();
  // Log the incoming message data to the console
  console.log("es5runner received message data:", String(data));
  // Key/value used for the example
  const key = "es5runner:example:key";
  const value = `hello from es5 runner ${new Date().toISOString()}`;

  try {
    // Execute SET command on redis service
    // The Call method signature is: Call(command, arg1, arg2, ...)
    const setRes = redis.call("SET", key, value);

    // Optionally verify by doing a GET
    const getRes = util.bytesToString(redis.call("GET", key));

    // Convert results to string and attach to message metadata.
    // The proxy returns the raw []byte result from the service; it
    // may already be a string depending on the Go<->JS conversion.
    message.setMetadata("redis.set.result", setRes);


    redis.call("JSON.SET", "es5runner:example:json", ".", `{"time":"${new Date().toISOString()}"}`);

    redis.call("JSON.MERGE", "es5runner:example:json", ".", `{"updated":"${new Date().toISOString()}"}`);

    const jsonRes = util.bytesToString(redis.call("JSON.GET", "es5runner:example:json"));

    message.setMetadata("redis.json.result", jsonRes);

    const newData = String(getRes) + "\n" + String(data);

    message.setDataString(newData);
  } catch (err) {
    // Record the error on the message metadata so the runner can see it
    message.setMetadata("es5runner.error", String(err));
    // Re-throw to make the runner treat this as a script failure if desired
    throw err;
  }
})();
