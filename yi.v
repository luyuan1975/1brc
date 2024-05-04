import io   { new_buffered_reader }
import math { min, max }
import os
import runtime { nr_cpus }
import time    { now }

const (
	fname   = '/tmp/measurements.txt'
	t_start = now()
	worker  = nr_cpus()
)

struct Station {
	min   f64
	max   f64
	sum   f64
	count i64
}

fn main() {
	if !os.exists(fname) { panic('file not exists!') }

	fsize := i64(os.file_size(fname))
	// println( '$fsize' )

	mut chunks := process_chunk(0, fsize, worker)!
	// println( chunks.str() )

	mut i := 1
	mut threads := []thread map[string]Station{}
	for {
		threads << spawn process_file(chunks[i-1], chunks[i])
		i++
		if chunks.len == i { break }
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
		println('${city} : ${re.min} : ${re.max} : ${re.sum / re.count} : ${re.count}')
	}
	println('city number : ${sorted_keys.len}')
	println( now() -  t_start )
}

fn process_file(start i64, end i64) map[string]Station {
	mut stations := map[string]Station{}

	mut chunks := process_chunk(start, end, worker) or { return stations }
	// println( chunks.str() )

 	mut i := 1
	mut fp := os.open_file(fname, 'r') or { return stations }
	for {
		leng := int(chunks[i] - chunks[i-1] - 1)
		fp.seek(chunks[i-1], .start) or { return stations }
		mut buf := []u8{ len: leng }
		mut br := io.new_buffered_reader(reader: fp, cap: leng)
		br.read(mut buf) or { return stations }

		lines := buf.bytestr().split('\n')
		for line in lines {
			data := line.split(';')
			city := data[0]
			temp := data[1].f64()

			re := stations[city] or {
				stations[city] = Station{ temp, temp, temp, 1 }
				continue
			}
			stations[city] = Station{
				min(temp, re.min), 
				max(temp, re.max), 
				re.sum + temp, 
				re.count + 1}
		}
		i++
		if i == chunks.len { break }
	}
	fp.close()
	// println( now() -  t_start )
	return stations
}

fn process_chunk(start i64, end i64, worker int)! []i64 {
	// mut chunks := [][]i64{ len: thread + 1, init: []i64{ len: 2 } }
	mut chunks := [ start ]

	mut chunk_start := start
	chunk_size := (end - start + worker - 1) / worker

	mut fp := os.open_file(fname, 'r') or { return chunks }
	for {
		chunk_start += chunk_size
		if chunk_start >= end { 
			chunks << end
			break
		}
		fp.seek(chunk_start, .start) or { return chunks }
		mut br := new_buffered_reader(reader: fp)
		line := br.read_line() or { return chunks }
		chunk_start += line.len + 1
		chunks << chunk_start
	}
	fp.close()
	return chunks
}
