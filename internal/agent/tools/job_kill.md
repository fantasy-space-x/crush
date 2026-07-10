Terminate a background shell process.

<usage>
- Provide the shell ID returned from a background bash execution
- Sends a cancellation request and cleans up resources
</usage>

<features>
- Stop long-running background processes
- Clean up completed background shells
- Waits up to 5 seconds for the process to terminate, then ignores the remaining kill result
</features>

<tips>
- Use this when you need to stop a background process
- The process receives a termination request (similar to SIGTERM)
- After killing, the shell ID becomes invalid
</tips>
