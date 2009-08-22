=head1 NAME

DBIx::ScaleOut::DBSet - pick and connect to the proper dbinsts

=head1 SYNOPSIS

  You'll probably just 'use DBIx::ScaleOut' and may never need to
  use this module directly.

=cut

package DBIx::ScaleOut::DBSet;

use strict;
use warnings;

#========================================================================

sub new {
	my($class, $projinst) = @_;

	# Get a list of all dbinst's referenced by this projinst.
	die "no projinst" if !$projinst;
	die "global not defined" if !$DBIx::ScaleOut::global;
	my $iinstset = $DBIx::ScaleOut::global->{projinst}{$projinst}{iinstset};
	die "no iinstset defined for '$projinst'" if !$iinstset;
	my %dbinst = ( );
	sub _record_dbinst {
		my($iinstset, $projinst, $shard, $purpose, $gen, $duples) = @_;
		for my $duple (@$duples) {
			my $dbinst = $duple->[1];
			$dbinst{$dbinst} = 0;
		}
	}
	foreach_purpose($iinstset, $projinst, \&_record_dbinst);
	my @dbinsts = sort keys %dbinst;

	# If any dbinst's are listed in the purpose that are
	# not loadable, throw a fatal error.
	for my $dbinst (@dbinsts) {
		next if $dbinst{$dbinst};
		# This returns the connection parameter hashref,
		# but we don't need that data, we just want to
		# make sure the require doesn't die.
		$dbinst{$dbinst} = DBIx::ScaleOut::get_dbinst($dbinst);
	}

	# Do a basic check for all dbinst's:  for those reached over
	# the network, a TCP ping, and for those reached over local
	# sockets, make sure the sock exists.
	# (Need to modularize this;  Setup needs to use it)
	my $ping = Net::Ping->new('tcp', 1); # some code duplication with Setup.pm
	for my $dbinst (@dbinsts) {
		next unless $dbinst{$dbinst};
		if ($dbinst->{host}) {
			my $host = $dbinst->{host};
			next if $host eq 'localhost' || $host eq '127.0.0.1';
			if (!$ping->ping($host)) {
				# Nonfatal error, hopefully a transient
				# network glitch.
				warn "cannot TCP ping '$host' for '$dbinst' in '$projinst'";
			}
		} elsif ($dbinst->{sock}) {
			my $sock = $dbinst->{sock};
			if (!-e $sock) {
				# Nonfatal error -- hopefully the DB process
				# just hasn't been started yet.
				warn "socket file not present '$sock' for '$dbinst' in '$projinst'";
			if (!-S _) {
				# File being the wrong type is odd enough that
				# it qualifies as a fatal error.
				die "not a socket file '$sock' for '$dbinst' in '$projinst'";
			}
			if (!-r _ || !-w _) {
				# This probably signifies permission problems,
				# so I'm also calling this a fatal error.
				die "socket file '$sock' not read/writeable for '$dbinst' in '$projinst'";
			}
		} else {
			# Setup should catch this condition before it's
			# allowed to be written.
			die "neither port nor sock for '$dbinst' in '$projinst'";
		}
	}

	my $self = bless {
		projinst =>	$projinst,
		dbinsts =>	\@dbinsts,
#		purpose =>	\%{ $purpose },
	}, $class;
	return $self;
}

# A convenience function (not method).  Given the nested hashref
# that makes up purposes, call the callback_func on every
# dbinst within it.
# The $projinst param is only there because it's convenient for
# the callback to have it, e.g. for error reporting.
sub foreach_purpose {
	my($shards, $projinst, $callback_func) = @_;
	return unless $purposes;
	my @shards = sort keys %$shards;
	for my $shard (@shards) {
		my @purposes = sort keys %{$purposes->{$shard}};
		for my $purpose (@purposes) {
			my $gens = $purposes->{$shard}{$purpose};
			for my $gen (0..$#$gens) {
				my $duples = $purposes->{$shard}{$purpose}[$gen];
				&$callback_func($purposes, $projinst,
					$shard, $purpose, $gen,
					$duples);
			}
		}
	}
}

# This is the main reason for this class:  to scan through a purpose,
# randomly pick a working (connect-able) dbinst, connect to it,
# and return it.
# Should have an option here to allow to connect non-cached.
# Might want an optimization for when only one dbinst is available
# for a particular shard and purpose (as will typically be the case
# at least half the time ('w') unless multiple masters are used as
# simultaneous writers).

sub get {
	my($self, $shard, $purpose) = @_;
	my $gens = $self->{purpose}{$shard}{$purpose};
	my($dbinst, $dbh) = ('', undef);
	my %dbinsts_tried = ( );
	DUPLE: for my $duples (@$gens) {
		my @dbinsts = pick_order($duples);
		TRY: for my $dbinst_try (@dbinsts) {
			if ($dbinsts_tried{$dbinst_try}) {
				# This means there is a minor error in
				# the specification for this purpose:
				# the same dbinst appears more than once.
				warn "will not retry '$dbinst_try' for $self->{projinst} $shard $purpose";
				next TRY;
			}
			my $dbh_try = DBIx::ScaleOut::connect_cached_dbinst(
				$dbinst, $self->{projinst});
			if ($dbh_try) {
				($dbinst, $dbh) = ($dbinst_try, $dbh_try);
				last DUPLE;
			} else {
				# Here, might want to mark the dbinst as
				# down in a class global, and code above
				# to refuse to retry connecting to it
				# for n seconds.
			}
			$dbinsts_tried{$dbinst_try} = 1;
		}
	}
	if ($dbh) {
		
	}
	if (wantarray) {
		return ($dbh, $dbinst);
	} else {
		return $dbh;
	}
}

# I might want a get_reader_and_writer method...?
# often the reader and writer will be the same dbinst.
# In that case the dbh can be gotten once and returned twice, rather than
# making DBI->connect_cached() have to recognize that they have the same
# params and making some extra function calls to do the same thing.
# Probably not a big concern.

# Passed an arrayref of [x, y] duples, where each x <= 1 is the
# probability of returning y.  Returns a randomized order to
# try picking them in.

sub pick_order {
	my($duples) = @_;
	return ( $duples->[0][1] ) if scalar(@$duples) == 1; # The most common case.
	my @d = @$duples;
	my $max = 1;
	my @list = ( );
	DUPLE: while (scalar(@d) > 1) {
		my $r = rand($max);
		# Subtract off each duple's probability in turn.
		# The one that takes it under 0 is the one picked.
		for my $i (0 .. $#d-1) {
			my $chance = $d[$i][0];
			$r -= $chance;
			if ($r < 0) {
				$max -= $chance;
				push @list, (splice @d, $i, 1)->[1];
				next DUPLE;
			}
		}
		# Maybe a floating point rounding error.
		$max -= $d[-1][0];
		push @list, (pop @d)->[1];
	}
	# When we get down to one item, the choice is easy.
	push @list, (pop @d)->[1];
	return @list;
}

1;

