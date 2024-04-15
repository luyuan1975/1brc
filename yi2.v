import io   { new_buffered_reader }
import math { min, max }
import os
import runtime { nr_cpus }
import time    { now }

const t_start = now()
const worker  = nr_cpus()

const fname   = '/tmp/measurements.txt'

struct Station {
	min   f64
	max   f64
	sum   f64
	count i64
}

fn main() {
	if !os.exists(fname) { panic('file not exists!') }

	fsize := i64(os.file_size(fname))
	chunk_size := (fsize + worker - 1) / worker
	// println( '$fsize : $chunk_size' )

	mut chunks := []i64{ len : worker + 1 }

	mut i := 1
	chunks[0] = 0
	mut fp := os.open_file(fname, 'r')!
	for {
		len := chunk_size * i
		mut br := io.new_buffered_reader(reader: fp)
		fp.seek(len, .start)!
		line := br.read_line()!
		// println('i=$i, $line')
		chunks[i] = len + line.len + 1
		i++
		if i == worker { break }
	}
	chunks[i] = fsize
	fp.close()
	// println(chunks.str())
	i = 0
	mut threads := []thread map[string]Station{}
	for {
		threads << spawn process_chunk(chunks[i], chunks[i+1])
		i++
		if worker == i { break }
	}
	mut res := threads.wait()

	mut stations := map[string]Station{}
	for re in res {
		for city, val in re {
			entry := stations[city] or {
				stations[city] = val
				continue
			}
			stations[city] = Station{
				min(val.min, entry.min), 
				max(val.max, entry.max), 
				val.sum    + entry.sum, 
				val.count  + entry.count }
		}
	}

	mut sorted_keys := stations.keys()
	sorted_keys.sort_ignore_case()

	for city in sorted_keys {
		re := stations[city]
		println('${city} ${re.min} : ${re.max} : ${re.sum / re.count} : ${re.count}')
	}
	println('city number : ${sorted_keys.len}')
	println( now() -  t_start )
}

fn process_chunk(begin i64, end i64) map[string]Station {
	mut stations := map[string]Station{}

	mut fp := os.open_file(fname, 'r') or { return stations }
	fp.seek(begin, .start) or { return stations }

	mut current := begin
	mut br := io.new_buffered_reader(reader: fp)
	for {
		if current >= end { break }
		line := br.read_line() or { break }
		data := line.split(';')
		city := data[0]
		temp := data[1].f64()
		current += line.len + 1

		re := stations[city] or {
			stations[city] = Station{temp, temp, temp, 1}
			continue
		}
		stations[city] = Station{
			min(temp, re.min), 
			max(temp, re.max), 
			re.sum + temp, 
			re.count + 1}
	}
	fp.close()
	println( now() -  t_start )
	return stations
}
