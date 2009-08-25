package DBIx::ScaleOut::Driver::mysql;

sub get_dsn {
	my($class, $dbinst) = @_;
	my %data = (
		database => $dbinst->{database},
	);
	if ($dbinst->{socket}) {
		# not really sure if this is correct
		$data{socket} = $dbinst->{socket};
	} else {
		$data{host} = $dbinst->{host};
		$data{port} = $dbinst->{port};
	}
	my $data_str = join ';', map { "$_=$data{$_}" } sort keys %data;
	$data_str .= ";attributes=$data{attributes}" if length $data{attributes};
use Data::Dumper; print STDERR "data_str: $data_str for dbinst: " . Dumper($dbinst);
	return "DBI:mysql:$data_str";
}

sub get_dtd {
	my($class, $dbh, $table) = @_;
	# regex $table here to prevent SQL injection
	my $ar = $dbh->selectrow_arrayref(qq{SHOW CREATE TABLE $table});
	return undef if !$ar || !@$ar || $ar->[0] ne $table;
	my $text = $ar->[1];
	# process $text into some kind of data structure... look around,
	# somebody has surely written a module related to this, right?
	return { };
}

1;

