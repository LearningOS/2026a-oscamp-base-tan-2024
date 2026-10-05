use core::cell::UnsafeCell;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicU32, Ordering};

const READER_MASK: u32 = (1 << 30) - 1;
const WRITER_HOLDING: u32 = 1 << 30;
const WRITER_WAITING: u32 = 1 << 31;

pub struct RwLock<T> {
    state: AtomicU32,
    data: UnsafeCell<T>,
}

unsafe impl<T: Send> Send for RwLock<T> {}
unsafe impl<T: Send + Sync> Sync for RwLock<T> {}

impl<T> RwLock<T> {
    pub const fn new(value: T) -> Self {
        Self {
            state: AtomicU32::new(0),
            data: UnsafeCell::new(value),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        loop {
            let state = self.state.load(Ordering::Acquire);

            // 有 writer 正在持有或等待时，
            // 新 reader 不允许进入，保证 writer priority。
            if state & (WRITER_HOLDING | WRITER_WAITING) != 0 {
                core::hint::spin_loop();
                continue;
            }

            let readers = state & READER_MASK;

            if readers == READER_MASK {
                core::hint::spin_loop();
                continue;
            }

            match self.state.compare_exchange(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return RwLockReadGuard { lock: self };
                }
                Err(_) => {
                    core::hint::spin_loop();
                }
            }
        }
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        // 告诉后续 reader：已经有 writer 在等待。
        self.state
            .fetch_or(WRITER_WAITING, Ordering::AcqRel);

        loop {
            let state = self.state.load(Ordering::Acquire);

            let readers = state & READER_MASK;
            let writer_holding = state & WRITER_HOLDING != 0;

            if readers != 0 || writer_holding {
                core::hint::spin_loop();
                continue;
            }

            // 当前应该只剩 WRITER_WAITING。
            match self.state.compare_exchange(
                WRITER_WAITING,
                WRITER_HOLDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return RwLockWriteGuard { lock: self };
                }
                Err(_) => {
                    core::hint::spin_loop();
                }
            }
        }
    }
}

pub struct RwLockReadGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> Drop for RwLockReadGuard<'_, T> {
    fn drop(&mut self) {
        self.lock
            .state
            .fetch_sub(1, Ordering::Release);
    }
}

pub struct RwLockWriteGuard<'a, T> {
    lock: &'a RwLock<T>,
}

impl<T> Deref for RwLockWriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for RwLockWriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for RwLockWriteGuard<'_, T> {
    fn drop(&mut self) {
        self.lock
            .state
            .fetch_and(!WRITER_HOLDING, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_read() {
        let lock = RwLock::new(42);

        let guard = lock.read();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_basic_write() {
        let lock = RwLock::new(10);

        {
            let mut guard = lock.write();
            *guard = 20;
        }

        let guard = lock.read();
        assert_eq!(*guard, 20);
    }

    #[test]
    fn test_multiple_readers() {
        let lock = Arc::new(RwLock::new(123));

        let mut handles = Vec::new();

        for _ in 0..10 {
            let lock = Arc::clone(&lock);

            handles.push(thread::spawn(move || {
                let guard = lock.read();
                assert_eq!(*guard, 123);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_multiple_writers() {
        let lock = Arc::new(RwLock::new(0usize));

        let mut handles = Vec::new();

        for _ in 0..10 {
            let lock = Arc::clone(&lock);

            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = lock.write();
                    *guard += 1;
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*lock.read(), 1000);
    }

    #[test]
    fn test_read_after_write() {
        let lock = Arc::new(RwLock::new(0));

        {
            let mut guard = lock.write();
            *guard = 99;
        }

        let mut handles = Vec::new();

        for _ in 0..5 {
            let lock = Arc::clone(&lock);

            handles.push(thread::spawn(move || {
                let guard = lock.read();
                assert_eq!(*guard, 99);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}