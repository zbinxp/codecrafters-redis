[![progress-banner](https://backend.codecrafters.io/progress/redis/a5d79c6a-968e-4388-bd3d-2beb7c39bd7f)](https://app.codecrafters.io/users/zbinxp?r=2qF)

This is a starting point for Rust solutions to the
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

In this challenge, you'll build a toy Redis clone that's capable of handling
basic commands like `PING`, `SET` and `GET`. Along the way we'll learn about
event loops, the Redis protocol and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Redis implementation is in `src/main.rs`. Study and
uncomment the relevant code, then run the command below to execute the tests on
our servers:

```sh
codecrafters submit
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `cargo (1.96)` installed locally
1. Run `./your_program.sh` to run your Redis server, which is implemented in
   `src/main.rs`. This command compiles your Rust project, so it might be slow
   the first time you run it. Subsequent runs will be fast.
1. Run `codecrafters submit` to submit your solution to CodeCrafters. Test
   output will be streamed to your terminal.

# la7

```
   SET key value   →  +OK\r\n                                                                    
   GET key         →  $<len>\r\n<value>\r\n      (存在)                                          
                   →  $-1\r\n                     (不存在，null bulk string) 
```