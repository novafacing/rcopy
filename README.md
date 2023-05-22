# rcopy

This program does one thing and one thing only! It remotely copies files or
directories as fast as I can figure out how over the network from one machine to
another. On my network, it hits several hundred MB/s (or 30GB in 90s).

`rcopy` uses a few key technologies to accomplish this speed:

- Asynchronous concurrent Rust using Tokio
- Zero-copy serialization and deserialization using RKYV
- Fast-as-possible file reads and writes using io_uring

If you know how to make it faster, please PR!

## Install

Install with `cargo install rcopy`

## Run

On the machine that has the file or directory (in my case, `~/hub/models/`...Vicuna may
be smaller than GPT-4 but the file is still pretty huge!) you want to copy, run:

```sh
rcopy send ~/hub/models
```

[![asciicast](https://asciinema.org/a/9kuAbjeIcXDPiCisptxlAU96S.svg)](https://asciinema.org/a/9kuAbjeIcXDPiCisptxlAU96S)

On the machine you want the files to copy to (we'll copy them to
`~/Downloads/models-copy/`), run:

```sh
rcopy receive -a 192.168.0.185:3120 ~/Downloads/models-copy/
```

[![asciicast](https://asciinema.org/a/jZNm0jeb6bOzGmvYzxE3LpCj6.svg)](https://asciinema.org/a/jZNm0jeb6bOzGmvYzxE3LpCj6)

## More Options

There are a few knobs to twist, for example 

## FAQ

Q: Is this secure?

A: No! Secure your network, this is for moving big model files that you don't care about
   from one machine to another. In the future, I plan on adding an encrypted mode
   that'll use the SSH keys already on your machines to negotiate AES.

Q: What's the use case for this?

A: Already mentioned above, but I literally am tired of moving Linux ISOs and 9GB ggml
   files between my machines. I realized SFTP and RSync are horrifically slow, so I
   figured it'd be fun to make my own.

## Planned Features

- [ ] Encryption (for obvious reasons)
- [ ] Holepunching (for non-LAN transactions)
- [ ] Data compression (for slower networks)
- [ ] Integrity checks (for careful people)
- [ ] Experiments with multiple TCP streams to find saturation limits