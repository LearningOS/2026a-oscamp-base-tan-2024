//! # Channel Communication
//!
//! In this exercise, you will use `std::sync::mpsc` channels to pass messages between threads.
//!
//! ## Concepts
//! - `mpsc::channel()` creates a multiple producer, single consumer channel
//! - `Sender::send()` sends a message
//! - `Receiver::recv()` receives a message
//! - Multiple producers can be created via `Sender::clone()`

use std::sync::mpsc;
use std::thread;

/// Create a producer thread that sends each element from items into the channel.
/// The main thread receives all messages and returns them.
pub fn simple_send_recv(items: Vec<String>) -> Vec<String> {
    let (tx, rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        for item in items {
            tx.send(item).unwrap();
        }
    });

    let mut result = Vec::new();

    for item in rx {
        result.push(item);
    }

    handle.join().unwrap();

    result
}

pub fn multi_producer(n_producers: usize) -> Vec<String> {
    let (tx, rx) = mpsc::channel();

    let mut handles = Vec::new();

    for id in 0..n_producers {
        let tx_clone = tx.clone();

        let handle = thread::spawn(move || {
            tx_clone
                .send(format!("msg from {}", id))
                .unwrap();
        });

        handles.push(handle);
    }

    drop(tx);

    let mut result: Vec<String> = rx.into_iter().collect();

    for handle in handles {
        handle.join().unwrap();
    }

    result.sort();

    result
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_send_recv() {
        let items = vec!["hello".into(), "world".into(), "rust".into()];
        let result = simple_send_recv(items.clone());
        assert_eq!(result, items);
    }

    #[test]
    fn test_simple_empty() {
        let result = simple_send_recv(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_multi_producer() {
        let result = multi_producer(3);
        assert_eq!(
            result,
            vec![
                "msg from 0".to_string(),
                "msg from 1".to_string(),
                "msg from 2".to_string(),
            ]
        );
    }

    #[test]
    fn test_multi_producer_single() {
        let result = multi_producer(1);
        assert_eq!(result, vec!["msg from 0".to_string()]);
    }
}
