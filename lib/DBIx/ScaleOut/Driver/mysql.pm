package DBIx::ScaleOut::Driver::mysql;

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

