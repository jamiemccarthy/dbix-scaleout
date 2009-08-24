package DBIx::ScaleOut::Base;

# This package is intended to be subclassed by your application,
# though you don't have to.  Probably you will use multiple subclasses,
# each defining methods specific to different parts of your code.
#
# Each subclass gets a different class instance.  But all the
# subclasses share some cached data.
#
# Your application probably will have no need to call new().
# You'll call db(), the class method exported by DBIx::ScaleOut.
# That will obtain the correct parameters from the appropriate
# subclass of ::Base and call new() if necessary to populate
# the cache.

use DBIx::ScaleOut;

# Subclasses will often override these class methods.
sub default_shard	{ 'main' }
sub default_purpose	{ 'w'    }

sub new {
	my($class, $shard, $purpose, $options) = @_;

	die "startup() has not been called"
		if !$DBIx::ScaleOut::Global;

	my $Global = $DBIx::ScaleOut::Global;

	# If a projinst isn't specified, default to the current projinst.
	my $projinst = $options->{projinst} || $Global->{default}{projinst};
	die "startup() has not been called for projinst '$projinst'"
		if !$Global->{projinst}{$projinst};

	$shard    ||= default_shard();
	$purpose  ||= default_purpose();
	$options  ||= { };

	# Set up the new object we'll be returning.  Don't try to connect
	# to anything yet;  see db().
	my $self = bless {
		projinst        => $projinst,
		shard           => $shard,
		purpose         => $purpose,
		opts            => $opts,
		rollcount       => 0,
		dbinst		=> '',
		dbh		=> undef,
		dbpurpose	=> '',
	}, $class;

	# Don't try yet to open any connections;  they'll be opened as
	# necessary.  So new() always succeeds.
	return $self;
}

sub connect {
	my($self, $purpose, $force_check) = @_;
	die "need purpose" unless $purpose && $purpose =~ /^[rw]$/;

	

	my $dbset = $DBIx::ScaleOut::Global->{projinst}{ $self->{projinst} }{dbset};
	# given dbset (which implies projinst), shard, and purpose,
	# the dbset can return us a dbinst, and we open our own dbh

}

1;

