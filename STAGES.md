# CodeCrafters Redis — full stage list

> Reconstructed in **platform order** (7 core stages, then extensions in order).
> Order verified against `codecrafters task -stage +N` (boundaries: cc3, fn4, ic1, jn4).
> Source: codecrafters-io/build-your-own-redis course-definition. Generated: 2026-09-27.
>
> **Progress: 83 done.** core #1–7, Lists #8–18, Streams #19–31,
> Transactions #32–42, Optimistic Locking #43–50, Replication #51–68.
> Pub/Sub #85–91 and Sorted Sets #92–99 were implemented **out of order** and are done.
> **NEXT: `zg5` — RDB file config (#69)**, then AOF #75–84, Bitmaps #100–108,
> Geospatial #109–116, Authentication #117–124.

| # | Stage | Slug | Group | Status |
|---|-------|------|-------|--------|
| 1 | Bind to a port | `jm1` | core | done |
| 2 | Respond to PING | `rg2` | core | done |
| 3 | Respond to multiple PINGs | `wy1` | core | done |
| 4 | Handle concurrent clients | `zu2` | core | done |
| 5 | Implement the ECHO command | `qq0` | core | done |
| 6 | Implement the SET & GET commands | `la7` | core | done |
| 7 | Expiry | `yz1` | core | done |
| 8 | Create a list | `mh6` | Lists | done |
| 9 | Append an element | `tn7` | Lists | done |
| 10 | Append multiple elements | `lx4` | Lists | done |
| 11 | List elements (positive indexes) | `sf6` | Lists | done |
| 12 | List elements (negative indexes) | `ri1` | Lists | done |
| 13 | Prepend elements | `gu5` | Lists | done |
| 14 | Query list length | `fv6` | Lists | done |
| 15 | Remove an element | `ef1` | Lists | done |
| 16 | Remove multiple elements | `jp1` | Lists | done |
| 17 | Blocking retrieval | `ec3` | Lists | done |
| 18 | Blocking retrieval with timeout | `xj7` | Lists | done |
| 19 | The TYPE command | `cc3` | Streams | done |
| 20 | Create a stream | `cf6` | Streams | done |
| 21 | Validating entry IDs | `hq8` | Streams | done |
| 22 | Partially auto-generated IDs | `yh3` | Streams | done |
| 23 | Fully auto-generated IDs | `xu6` | Streams | done |
| 24 | Query entries from stream | `zx1` | Streams | done |
| 25 | Query with - | `yp1` | Streams | done |
| 26 | Query with + | `fs1` | Streams | done |
| 27 | Query single stream using XREAD | `um0` | Streams | done |
| 28 | Query multiple streams using XREAD | `ru9` | Streams | done |
| 29 | Blocking reads | `bs1` | Streams | done |
| 30 | Blocking reads without timeout | `hw1` | Streams | done |
| 31 | Blocking reads using $ | `xu1` | Streams | done |
| 32 | The INCR command (1/3) | `si4` | Transactions | done |
| 33 | The INCR command (2/3) | `lz8` | Transactions | done |
| 34 | The INCR command (3/3) | `mk1` | Transactions | done |
| 35 | The MULTI command | `pn0` | Transactions | done |
| 36 | The EXEC command | `lo4` | Transactions | done |
| 37 | Empty transaction | `we1` | Transactions | done |
| 38 | Queueing commands | `rs9` | Transactions | done |
| 39 | Executing a transaction | `fy6` | Transactions | done |
| 40 | The DISCARD command | `rl9` | Transactions | done |
| 41 | Failures within transactions | `sg9` | Transactions | done |
| 42 | Multiple transactions | `jf8` | Transactions | done |
| 43 | The WATCH command | `jb7` | Optimistic Locking | done |
| 44 | WATCH inside transaction | `jq9` | Optimistic Locking | done |
| 45 | Tracking key modifications | `mh8` | Optimistic Locking | done |
| 46 | Watching multiple keys | `fp0` | Optimistic Locking | done |
| 47 | Watching missing keys | `uo9` | Optimistic Locking | done |
| 48 | The UNWATCH command | `bn1` | Optimistic Locking | done |
| 49 | Unwatch on EXEC | `fn4` | Optimistic Locking | done |
| 50 | Unwatch on DISCARD | `hq1` | Optimistic Locking | done |
| 51 | Configure listening port | `bw1` | Replication | done |
| 52 | The INFO command | `ye5` | Replication | done |
| 53 | The INFO command on a replica | `hc6` | Replication | done |
| 54 | Initial replication ID and offset | `xc1` | Replication | done |
| 55 | Send handshake (1/3) | `gl7` | Replication | done |
| 56 | Send handshake (2/3) | `eh4` | Replication | done |
| 57 | Send handshake (3/3) | `ju6` | Replication | done |
| 58 | Receive handshake (1/2) | `fj0` | Replication | done |
| 59 | Receive handshake (2/2) | `vm3` | Replication | done |
| 60 | Empty RDB transfer | `cf8` | Replication | done |
| 61 | Single-replica propagation | `zn8` | Replication | done |
| 62 | Multi-replica propagation | `hd5` | Replication | done |
| 63 | Command processing | `yg4` | Replication | done |
| 64 | ACKs with no commands | `xv6` | Replication | done |
| 65 | ACKs with commands | `yd3` | Replication | done |
| 66 | WAIT with no replicas | `my8` | Replication | done |
| 67 | WAIT with no commands | `tu8` | Replication | done |
| 68 | WAIT with multiple commands | `na2` | Replication | done |
| 69 | RDB file config | `zg5` | RDB Persistence |  |
| 70 | Read a key | `jz6` | RDB Persistence |  |
| 71 | Read a string value | `gc6` | RDB Persistence |  |
| 72 | Read multiple keys | `jw4` | RDB Persistence |  |
| 73 | Read multiple string values | `dq3` | RDB Persistence |  |
| 74 | Read value with expiry | `sm4` | RDB Persistence |  |
| 75 | Default AOF options | `uj3` | AOF Persistence |  |
| 76 | AOF options from flags | `vd9` | AOF Persistence |  |
| 77 | Create append-only directory | `fm0` | AOF Persistence |  |
| 78 | Create append-only file | `dw4` | AOF Persistence |  |
| 79 | Create manifest file | `pb9` | AOF Persistence |  |
| 80 | Write a single command | `dc8` | AOF Persistence |  |
| 81 | Write multiple commands | `fi1` | AOF Persistence |  |
| 82 | Filter write commands | `ep6` | AOF Persistence |  |
| 83 | Replay a single command | `xz2` | AOF Persistence |  |
| 84 | Replay multiple commands | `kn2` | AOF Persistence |  |
| 85 | Subscribe to a channel | `mx3` | Pub/Sub | done |
| 86 | Subscribe to multiple channels | `zc8` | Pub/Sub | done |
| 87 | Enter subscribed mode | `aw8` | Pub/Sub | done |
| 88 | PING in subscribed mode | `lf1` | Pub/Sub | done |
| 89 | Publish a message | `hf2` | Pub/Sub | done |
| 90 | Deliver messages | `dn4` | Pub/Sub | done |
| 91 | Unsubscribe | `ze9` | Pub/Sub | done |
| 92 | Create a sorted set | `ct1` | Sorted Sets | done |
| 93 | Add members | `hf1` | Sorted Sets | done |
| 94 | Retrieve member rank | `lg6` | Sorted Sets | done |
| 95 | List sorted set members | `ic1` | Sorted Sets | done |
| 96 | ZRANGE with negative indexes | `bj4` | Sorted Sets | done |
| 97 | Count sorted set members | `kn4` | Sorted Sets | done |
| 98 | Retrieve member score | `gd7` | Sorted Sets | done |
| 99 | Remove a member | `sq7` | Sorted Sets | done |
| 100 | Create a bitmap | `bq9` | Bitmaps |  |
| 101 | Retrieve a bit | `qj1` | Bitmaps |  |
| 102 | Read a string as bits | `yj2` | Bitmaps |  |
| 103 | Read bits as a string | `pk5` | Bitmaps |  |
| 104 | Grow a bitmap | `yf6` | Bitmaps |  |
| 105 | Count set bits | `nx3` | Bitmaps |  |
| 106 | AND two bitmaps | `hv4` | Bitmaps |  |
| 107 | AND bitmaps of different lengths | `dk2` | Bitmaps |  |
| 108 | OR two bitmaps | `fr8` | Bitmaps |  |
| 109 | Respond to GEOADD | `zt4` | Geospatial Commands |  |
| 110 | Validate coordinates | `ck3` | Geospatial Commands |  |
| 111 | Store a location | `tn5` | Geospatial Commands |  |
| 112 | Calculate location score | `cr3` | Geospatial Commands |  |
| 113 | Respond to GEOPOS | `xg4` | Geospatial Commands |  |
| 114 | Decode coordinates | `hb5` | Geospatial Commands |  |
| 115 | Calculate distance | `ek6` | Geospatial Commands |  |
| 116 | Search within radius | `rm9` | Geospatial Commands |  |
| 117 | Respond to ACL WHOAMI | `jn4` | Authentication |  |
| 118 | Respond to ACL GETUSER | `gx8` | Authentication |  |
| 119 | The nopass flag | `ql6` | Authentication |  |
| 120 | The passwords property | `pl7` | Authentication |  |
| 121 | Setting default user password | `uv9` | Authentication |  |
| 122 | The AUTH command | `hz3` | Authentication |  |
| 123 | Enforce authentication | `nm2` | Authentication |  |
| 124 | Authenticate using AUTH | `ws7` | Authentication |  |

Total: 124 stages
