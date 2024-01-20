use async_std::task;
use std::{fs::File, time::Instant, io::{BufReader, Read, self}, collections::HashMap};
use crossbeam::{channel::{self, Sender}, select};

#[derive(Clone, Debug)]
struct CityMetrics {
    pub low: f32,
    pub high: f32,
    pub temperature_sum: f32,
    pub mean: f32,
    pub num_temps: u32,
}

async fn async_tally(sender: Sender<HashMap<String, CityMetrics>>, data: Vec<u8>) {
    let mut cities:HashMap<String, CityMetrics> = HashMap::new();
    let mut start: usize = 0;
    for (newline_index, &byte) in data.iter().enumerate() {
        // line in read_lines() is soooooooo slooooooow. Resort to byte checking.
        if byte == b'\n' {
            if let Ok(line) = std::str::from_utf8(&data[start..newline_index]) {
                let (city_name, temp_str) = line.split_once(';').expect(format!("Could not find delimeter in line '{}'", line).as_str());
                let temperature = temp_str.parse::<f32>().expect(format!("Could not parse '{}' as f32", temp_str).as_str());
                cities.entry(city_name.to_string()).and_modify(|city_metrics|{
                    if temperature > city_metrics.high {
                        city_metrics.high = temperature;
                    } else if temperature < city_metrics.low {
                        city_metrics.low = temperature;
                    }
                    city_metrics.num_temps += 1;
                    city_metrics.temperature_sum += temperature;
                }).or_insert(
                    CityMetrics{
                        high: temperature,
                        low: temperature,
                        mean: 0.0,
                        temperature_sum: 0.0,
                        num_temps: 1,
                    }
                );
            }
            start = newline_index + 1;
        }
    }

    sender.send(cities).expect("Failed to send a processed map of cities");
}

fn main() {    
    // Setup and print
    let file_path = "measurements.txt";
    let num_processors = num_cpus::get();
    let (sender, receiver) = channel::unbounded::<HashMap<String, CityMetrics>>();
    let mut num_tasks = 0;

    println!("Number of processors: {}", num_processors);
    println!("Reading in the {} file", file_path);
    let file_metadata = std::fs::metadata(file_path).expect(format!("Cannot access meta data in file {}, file_path", file_path).as_str());
    let file = File::open(file_path).expect("Could not read file");
    let mut reader = BufReader::new(file);
    let file_size: usize = file_metadata.len() as usize;
    let mut file_bytes_read: usize = 0;
    println!("File Size: {}", file_size);

    // Smaller chunks make it faster to process, square/scale it to the num of processors.
    let num_chunks = num_processors * num_processors;
    let chunk_size = (file_size as usize / num_chunks) + 1;
    let extra_chunky = 1024 * 10; // Allocate an extra 10 KB for each vector
    println!("Splitting the vector into equal sized chunks of: {}", chunk_size);

    let mut chunk_remainder = Vec::new();

    // Start execution
    let app_start_time = Instant::now();
    loop {
        // Chunking explained:
        // Read in the data as fast as possible, easiest method is as bytes.
        // Once it has been read in, the data is likely misaligned.
        // Re-align the data by truncating the current chunk
        // and pre-pending it to the next chunk. Avoid data copies
        // and REALLY avoid vec bumping.
        let mut chunk: Vec<u8> = vec![0; chunk_size + extra_chunky];
        let offset = chunk_remainder.len();
        if chunk_remainder.len() > 0 {
            chunk[..chunk_remainder.len()].copy_from_slice(&chunk_remainder);
            chunk_remainder.clear();
        }
        let bytes_read = reader.read(&mut chunk[offset..]);
        match bytes_read {
            Ok(num_bytes) => {
                if num_bytes == 0 {
                    break;
                }
                file_bytes_read += num_bytes;
                println!("Read file {} / {} bytes", file_bytes_read, file_size);

                if file_bytes_read < file_size {
                    if let Some(index) = chunk.iter().rev().position(|&x| x == b'\n') {
                        // The index is found, considering it's reversed, convert it to the original index
                        let original_index = chunk.len() - index;
                        chunk_remainder = chunk[(original_index)..chunk.len()].to_vec();
                        chunk.truncate(original_index);
                    } else {
                        println!("Error: Expected to find newline character in chunk");
                    }
                }

                let sender_clone = sender.clone();
                // ♥ async-std. Please, please, please keep this project going.
                let _ = task::spawn(async move {
                    async_tally(sender_clone, chunk).await
                });
                num_tasks += 1;
            }
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // EOF, unexpected but tolerable
                break;
            }
            Err(e) => {
                // Some other error, dunno
                println!("Failed to read file: {}", e.to_string());
                break;
            }
        }
    }
    let file_end_time  = Instant::now();
    let file_read_total_time = file_end_time - app_start_time;
    println!("Time spent reading in the file: {} ms", file_read_total_time.as_millis());
    
    let mut all_cities: HashMap<String, CityMetrics> = HashMap::new();
    let mut count_tasks_completed = 0;
    let total_tasks = num_tasks;
    loop {
        select! {
            recv(receiver) -> msg => {
                let cities = msg.expect("Main thread could not process message from child thread");
                count_tasks_completed += 1;
                println!("Tasks {} / {}, received a total of {} cities to compute", count_tasks_completed, total_tasks, cities.len());
                
                cities.into_iter().for_each(|(name, city)|{
                    all_cities.entry(name).and_modify(|city_tally| {
                        city_tally.num_temps += city.num_temps;
                        city_tally.temperature_sum += city.temperature_sum;
                        
                        if city.high > city_tally.high {
                            city_tally.high = city.high;
                        }
                        if city.low < city_tally.low {
                            city_tally.low = city.low;
                        }
                    }).or_insert(city.clone());
                });

                num_tasks -= 1;
                if num_tasks == 0 {
                    break;
                }
            },
        }
    }

    let mut total_num_temps = 0;
    all_cities.iter_mut().for_each(|(name, metric)|{
        metric.mean = metric.temperature_sum / metric.num_temps as f32;
        total_num_temps += metric.num_temps;
        println!(
        "City Name: {}
        \tHigh Temp: {}
        \tLow Temp: {}
        \tMean Temp: {:.1}",
        name, metric.high, metric.low, metric.mean);
    });

    let app_end_time = Instant::now();
    let app_total_time = app_end_time - app_start_time;
    // Processing time occurs inline with the file reading, however these
    // operations interfere with each other. The numbers will vary
    // depending on their interference.
    let estimated_time_processing = app_total_time - file_read_total_time;
    println!(
       "=================================\n\
       {:<30}{:>11} \n\
       {:<27}{:>8} ms\n\
       {:<27}{:>8} ms\n\
       {:<27}{:>8} ms",
       "Total Temperatures Processed: ",
       total_num_temps,
       "Time reading in file:",
       file_read_total_time.as_millis(),
       "Estimated Time processing:",
       estimated_time_processing.as_millis(),
       "Total time:",
       app_total_time.as_millis());
}