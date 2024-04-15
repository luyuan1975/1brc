# constant $thread = 4;
constant $thread = $*KERNEL.cpu-cores;

my $fname = '/tmp/measurements.txt';
die "file doesn't exist!" unless $fname.IO.e;

my $fsize = $fname.IO.s;
# say 'file size :', $fsize;

my @chunks = process_chunk(0, $fsize, $thread);
# say @chunks;

my @futures = @chunks.map({ start process_file($_); });
my @chunk_res = @futures.map({ await $_; });
# my @chunk_res = @chunks.race.map({ process_file($_); });

# my ($city, $val);
my ($re, %res);
for @chunk_res {
    for $_.kv -> $city, $val {
        if (%res{$city}:exists) {
            $re = %res{$city};
            $re[2] += $val[2];
            $re[3] += $val[3];
            $re[0] = $val[0] if $val[0] < $re[0];
            $re[1] = $val[1] if $val[1] > $re[1];
        } else {
            %res{$city} = $val;
        }
    }
}

my @keys = %res.keys.sort;
for @keys -> $city {
    $re = %res{$city};
    # [最小，最大，平均，次数]
    say $city, ' : ', $re[0], ', ', $re[1], ', ', $re[2]/$re[3], ', ', $re[3];
}
say @keys.WHAT, @keys.elems;

say 'time3 = ', now - INIT now;

sub process_file([$start, $end]) {
    # say 'start2 = ', $start, ' : end2 = ', $end;
    my ($re, %res); # [最小，最大，和，次数]
    my (@lines, $city, $temp);

    my @chunks = process_chunk($start, $end, $thread);
    # say @chunks;

    my $fp = open($fname, :r);
    for @chunks {
        $fp.seek($_[0]);
        my $data = $fp.read($_[1]-$_[0]).decode;
        @lines = $data.lines;
        for @lines {
            ($city, $temp) = $_.split(';');
            $temp = $temp.Numeric;
            if (%res{$city}:exists) {
                $re = %res{$city};
                $re[3]++;
                $re[2] += $temp;
                $re[0] = $temp if $temp < $re[0];
                $re[1] = $temp if $temp > $re[1];
            } else {
                %res{$city} = [$temp, $temp, $temp, 1];
            }
        };
    }
    $fp.close;
    say 'start = ', $start, ', end = ', $end, ', : time2 = ', now - INIT now;
    return %res;
}
sub process_chunk($start, $end, $thread) {
    my @chunks;

    my $chunk_end = 0;
    my $chunk_start = $start;
    my $chunk_size = ($end - $start + $thread - 1) div $thread;
    # say 'chunk_start = ', $start, ' : end = ', $end, ', chunk_size =', $chunk_size;

    my $fp = open($fname, :r);
    repeat {
        $chunk_end = min($end, $chunk_start + $chunk_size);
        $fp.seek($chunk_end);
        loop {
            last if $fp.eof;
            last if $fp.read(1) eq buf8.new(10);
        }
        $chunk_end = $fp.tell;
        @chunks.push( [ $chunk_start, $chunk_end - 1] );
        $chunk_start = $chunk_end;
    } until $chunk_start >= $end;
    $fp.close;
    # say @chunks;
    return @chunks;
}
