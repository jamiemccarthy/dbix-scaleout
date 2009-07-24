package DBIx::ScaleOut::Base;

# This package is intended to be subclassed by your project.
# Probably you will use multiple subclasses, each defining
# methods specific to different parts of your code.
#
# Each subclass gets a different class instance.  But all the
# subclasses share some cached data.

# Your application probably will have no need to call new().
# You'll call db(), the class method exported by DBIx::ScaleOut.
# That will obtain the correct parameters from the appropriate
# subclass of ::Base and call new() if necessary to populate
# the cache.

use DBIx::ScaleOut;
use DBIx::ScaleOut::Dispatch; # XXX ??

# Subclasses will often override these class methods.
sub default_shard	{ 'main' }
sub default_purpose	{ 'r'    }

sub new {
	my($class, $shard, $purpose) = @_;
	die "startup not called" if !$DBIx::ScaleOut::Global; # XXX do it automatically?
	return bless {
		shard =>		$shard,
		purpose =>		$purpose,
		rollcount =>		0,
		used_to_write =>	0,
	}, $class;
}

sub reroll {
	my($self) = @_;
	# XXX think this over
	$self->{rollcount} = $DBIx::ScaleOut::rollcount;
	$self->{used_to_write} = 0;
}

1;

