use std::time::Instant;

fn main() {
    let now = Instant::now();

    let mut i = 0;
    // we sleep for 2 seconds
    for _ in 1..1000000000 {
        i = i + 1;
    }
    // it prints '2'
    let time = now.elapsed().as_millis();
    println!("i {}", time);
}
