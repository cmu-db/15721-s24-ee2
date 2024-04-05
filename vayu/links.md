
taskset --cpu-list 0-3 your_program,  taskset --cpu-list 0,1,16,17 your_program

Velox Join 
https://facebookincubator.github.io/velox/develop/joins.html#

Testing on aws
https://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/dedicated-hosts-overview.html


https://docs.google.com/document/d/1txX60thXn1tQO1ENNT8rwfU3cXLofa7ZccnvP4jD6AA/edit#heading=h.3iwlbn2gzs29



# io_uring
https://ryanseipp.com/post/iouring-vs-epoll/

https://users.rust-lang.org/t/help-understanding-why-io-uring-io-performs-worse-than-stdlib-in-a-thread-pool/97853/3

https://questdb.io/blog/2022/09/12/importing-300k-rows-with-io-uring/

https://www.reddit.com/r/golang/comments/1709wck/uringnet_an_ultrafast_network_io_framework_with/
https://www.reddit.com/r/linux/comments/qm09rf/io_uring_based_networking_in_prod_experience/

# context switching 
https://unix.stackexchange.com/questions/681096/understanding-overhead-cost-of-context-switching

https://unix.stackexchange.com/questions/39342/how-to-see-how-many-context-switches-a-process-makes
https://unix.stackexchange.com/questions/259710/understanding-linux-perf-sched-switch-and-context-switches


# CPU affinifty
https://askubuntu.com/questions/483824/how-to-run-a-program-with-only-one-cpu-core
https://serverfault.com/questions/249754/how-to-configure-linux-for-using-only-one-cpu-core-of-a-numa-system
https://www.baeldung.com/linux/disable-hyperthreading
https://stackoverflow.com/questions/52298242/why-perf-has-such-high-context-switches
https://internals.rust-lang.org/t/pre-rfc-thread-affinity/3117/7
https://github.com/Elzair/core_affinity_rs/issues/7
https://stackoverflow.com/questions/53919482/whats-the-process-of-disabling-interrupt-in-multi-processor-system
https://stackoverflow.com/questions/8032372/how-can-i-see-which-cpu-core-a-thread-is-running-in
https://unix.stackexchange.com/questions/655293/how-to-check-if-how-often-my-process-is-preempted-by-the-kernel
https://unix.stackexchange.com/questions/439260/why-does-perf-stat-show-0-context-switches


# thread per core
