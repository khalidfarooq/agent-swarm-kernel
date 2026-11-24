/// Pins the calling OS thread to the logical CPU corresponding to `core_id`.
///
/// Returns `true` if the underlying OS affinity call succeeded, or `false` if unsupported
/// or rejected by the host kernel/scheduler.
#[cfg(target_os = "linux")]
pub fn pin_current_thread(core_id: usize) -> bool {
    unsafe {
        let mut allowed: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut allowed);

        let target_cpu = if libc::sched_getaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &mut allowed,
        ) == 0
        {
            let mut cpus = Vec::with_capacity(64);
            for cpu in 0..(libc::CPU_SETSIZE as usize) {
                if libc::CPU_ISSET(cpu, &allowed) {
                    cpus.push(cpu);
                }
            }
            if cpus.is_empty() {
                let n = std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(1);
                core_id % n
            } else {
                cpus[core_id % cpus.len()]
            }
        } else {
            let n = std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1);
            core_id % n
        };

        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(target_cpu, &mut cpuset);

        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset) == 0
    }
}

/// Pins the calling OS thread using Mach `THREAD_AFFINITY_POLICY` on macOS.
///
/// Returns `true` if `thread_policy_set` returns `KERN_SUCCESS` (`0`), or `false`
/// when the host architecture/kernel does not support affinity tags (e.g. Apple Silicon
/// returning `KERN_NOT_SUPPORTED`).
#[cfg(target_os = "macos")]
pub fn pin_current_thread(core_id: usize) -> bool {
    const THREAD_AFFINITY_POLICY: u32 = 4;
    const THREAD_AFFINITY_POLICY_COUNT: u32 = 1;
    const KERN_SUCCESS: i32 = 0;

    #[repr(C)]
    struct ThreadAffinityPolicyData {
        affinity_tag: i32,
    }

    extern "C" {
        fn mach_thread_self() -> u32;
        fn mach_task_self() -> u32;
        fn mach_port_deallocate(task: u32, name: u32) -> i32;
        fn thread_policy_set(
            thread: u32,
            flavor: u32,
            policy_info: *const ThreadAffinityPolicyData,
            count: u32,
        ) -> i32;
    }

    let n = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);
    let tag = ((core_id % n) as i32) + 1;
    let policy = ThreadAffinityPolicyData { affinity_tag: tag };

    unsafe {
        let thr = mach_thread_self();
        let kr = thread_policy_set(
            thr,
            THREAD_AFFINITY_POLICY,
            &policy as *const ThreadAffinityPolicyData,
            THREAD_AFFINITY_POLICY_COUNT,
        );
        mach_port_deallocate(mach_task_self(), thr);
        kr == KERN_SUCCESS
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn pin_current_thread(_core_id: usize) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pin_current_thread_does_not_panic() {
        let result = pin_current_thread(0);
        #[cfg(target_os = "linux")]
        assert!(result, "Linux sched_setaffinity should succeed for core 0");
        #[cfg(not(target_os = "linux"))]
        let _ = result;
    }
}
