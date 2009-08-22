=head1 NAME

DBIx::ScaleOut - a smart database access layer

=cut

package DBIx::ScaleOut;

use strict;
use warnings;

# If your code does a 'use DBIx::ScaleOut', these use's come along too.
# The most notable one is DBIx::ScaleOut::Base, which is the base class
# which your application's db classes will subclass.

use base 'Exporter';
use DBI;
use Storable;
use Cache::Memory;
use Cache::Memcached;

use vars qw( $Global );
our @EXPORT = qw(
	db
	dat
);

our($VERSION) = ' $Revision: 0.01 $ ' =~ /\$Revision:\s+([^\s]+)/;

#========================================================================

# This class global is expected to be created at e.g. Apache parent
# startup time, and to persist through Apache child forks.
our $Global = undef;

# Subclassing DBIx::ScaleOut to override this doesn't work unless
# you call YourClass->new() ???  To set something in $Global?  I dunno
# XXX figure out how to allow custom ::ScaleOut subclasses
sub default_projinst	{ 'main' }

#========================================================================

# expect db() to be called very frequently, make it fast when everything
# is already initialized properly

sub db {
	my($class, $options) = @_;

	startup(default_projinst()) if !$Global;

	$class ||= dat('dxso_classdefault') || 'DBIx::ScaleOut::Base';
	die "invalid class '$class' in db()" unless $class->isa('DBIx::ScaleOut::Base');

	my $shard = $options->{shard} || $class->default_shard;

	my $purpose = $options->{purpose} || $class->default_purpose;
	die "invalid purpose '$purpose' in db()" unless $purpose =~ /^[rw]$/;

	return
		$Global->{db_classinst_cache}{$class}{$shard}{$purpose}
			||= $class->create($shard, $purpose);
}

sub dat {
	my($name) = @_;
	return $name ? $Global->{dat}{$name} : $Global->{dat};
}

# startup:
#
# Intended to be called at e.g. Apache startup time (pre-fork) but
# for lazy programmers should work (just slower) when not called
# at all until db() is called.
#
# 'require' some modules that this project will need and initialize
# the global variable.
#
# then connect to writer, read constants (maybe thru ::Setup? Access?)
#
# then
# - read vars table
# - ping each defined dbinst except the writer and emit a warning
#   (not error, at this stage) for any failure.
#
# I really would (eventually) like a way to allow multiple projinst's
# but for now -- stage0 sets {default_projinst} and everything else
# just uses that.

sub startup {
	my($projinst) = @_;
	$projinst ||= default_projinst();

	my $class = 'DBIx::ScaleOut'; # XXX figure out how to allow custom ::ScaleOut subclasses
	$class->startup_stage0($projinst);
	$class->startup_stage1();
	$class->startup_ready();
}

sub reroll {
	$Global->{rollcount}++;
}

# stage0: require perl modules and create stub $Global

sub startup_stage0 {
	my($class, $projinst) = @_;

	my $setup_class = "DBIx::ScaleOut::Access::$projinst";
	require $setup_class; # If this dies, nothing is going to work anyway
	my $dbinsts_hr = $setup_class->dbinsts;
	my $initial = (grep { $dbinsts_hr->{$_}{initial} } sort keys %$dbinsts_hr)[0];
	my $driver = $setup_class->{driver} || die "no driver defined in '$projinst'";
	if (!grep { $_ eq $driver } DBI->available_drivers) { die "driver '$driver' not installed" }
	my $driver_class = "DBIx::ScaleOut::Driver::$driver";
	require $driver_class; # ditto

	$Global = {
		rollcount	=> 0,
		believed_pid	=> $$,
		default_projinst => $projinst,
		dbinst		=> $dbinsts_hr,
		initial_dbinst	=> $initial,
	};
}

# startup_stage1: connect to writer DB, retrieve constants table,
# retrieve vars table, create cache objects - all using DBI
# only, no DBIx::ScaleOut methods (maybe we can figure out
# a way to use them later to reduce repeated code, but for
# now, this is easiest)
#
# See Setup::check_dbinsts re code reuse

sub startup_stage1 {
	my($class) = @_;

	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	# when we enable multiple connection attempts with a connectorder
	# (in case the first choice writer db is down), the params should
	# be a list and the connection attempt should be in a loop
	my $initial_dbinst = $gp->{dbinst}{ $gp->{initial_dbinst} };

	my $initial_dbh = $class->startup_stage1_connect($initial_dbinst);
	die "startup_stage1 cannot connect" if !$initial_dbh;
	$class->startup_stage1_digestconstantstable($initial_dbinst, $initial_dbh);
	my $raw_constants = $class->startup_stage1_readconstants($initial_dbh);
	$class->startup_stage1_setconstants($initial_dbinst, $raw_constants);

	$class->startup_stage1_digestvariablestable($initial_dbh);
	my $raw_variables = $class->startup_stage1_readvariables($initial_dbh);
	$class->startup_stage1_setvariabledefaults($raw_variables);
	$class->startup_stage1_setdat($raw_variables);

	$class->startup_stage1_classinst();
	$class->startup_stage1_dbset();

	$class->startup_stage1_memcached();
	$class->startup_stage1_localcache();
}

# startup_ready: make any final changes to allow rest of methods to be called
# (not a bad place for subclasses to want to override)

sub startup_ready {
	my($class) = @_;

	$class->reroll();
}

sub startup_stage1_connect {
	my($class, $dbinst) = @_;
	return DBI->connect_cached($dbinst->{dsn}, $dbinst->{dbuser}, $dbinst->{password});
}

sub startup_stage1_digestconstantstable {
	my($class, $dbinst, $dbh) = @_;
	my $table_name = $dbinst->{constantstable};
	$class->startup_stage1_digesttable($dbh, $table_name);
}

sub startup_stage1_digesttable {
	my($class, $dbh, $table_name) = @_;
	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	my $driver_class = $gp->{driver_class};
	my $dtd = $driver_class->get_dtd($dbh, $table_name);
	my $digest = $class->digest_dtd($dtd);
	$gp->{digest}{$table_name} = $digest;
}

sub digest_dtd {
	my($class, $dtd) = @_;
	# should do something intelligent here. for now, the
	# dxso_constants and variables tables have to have their
	# key column named 'name' and value named 'value'
	return { keycol => 'name', valcol => 'value' };
}

sub startup_stage1_readconstants {
	my($class, $dbinst, $dbh) = @_;
	my $table_name = $dbinst->{constantstable};
	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	my $keycol = $gp->{digest}{$table_name}{keycol};
	my $valcol = $gp->{digest}{$table_name}{valcol};
	my $raw_constants = $dbh->selectall_hashref(
		"SELECT $keycol, $valcol FROM $table_name",
		$keycol);
	return $raw_constants;
}

sub startup_stage1_setconstants {
	my($class, $dbinst, $c) = @_;
	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	$c->{dxso_classdefault} ||= 'DBIx::ScaleOut::Base';
	# there can't be a constant (or var) overriding the name
	# of the constants table (for obvious reasons)
	$c->{dxso_constantstable} = $dbinst->{constantstable};
	$c->{dxso_variablestable} ||= 'dxso_variables';
	$c->{dxso_iinstsettable} ||= 'dxso_iinstset';
	# and set other defaults
	$gp->{constants} = \%$c;
}

sub startup_stage1_digestvariablestable {
	my($class, $dbh) = @_;
	my $table_name = $gp->{constants}{dxso_variablestable};
	$class->startup_stage1_digesttable($dbh, $table_name);
}

sub startup_stage1_readvariables {
	my($class, $dbh) = @_;
	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	my $table_name = $gp->{constants}{dxso_variables};
	my $keycol = $gp->{digest}{$table_name}{keycol};
	my $valcol = $gp->{digest}{$table_name}{valcol};
	my $raw_variables = $dbh->selectall_hashref(
		"SELECT $keycol, $valcol FROM $table_name",
		$keycol);
	return $raw_variables;
}

sub startup_stage1_setvariabledefaults {
	my($class, $v) = @_;
	my %defaults = (
		dxso_memcached		=> 0,
		dxso_memcached_servers	=> '127.0.0.1:11211',
		dxso_memcached_debug	=> 0,
		dxso_dat_expiretime	=> 300,
	);
	for my $key (keys %defaults) {
		$v->{$key} = $defaults->{$key} if !defined $v->{$key};
	}
}

sub startup_stage1_setdat {
	my($class, $raw_variables) = @_;
	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	my $constants = $gp->{constants};
	my $v = \%$constants;
	for my $key (keys %$raw_variables) {
		# Data pulled from constants used to get this far
		# cannot be overwritten.
		next if key =~ /^dxso_(constantstable|variablestable)$/;
		# Other variables do overwrite constants.
		$v->{$key} = $raw_variables->{$key};
	}
	# actually, need to use some kind of generic caching mechanism
	# here -- store the expiration time, determine expiration time
	# by the dat 'dxso_dat_expiretime', write to memcached if
	# available
	$gp->{dat} = $v;
}

sub startup_stage1_classinst {
	$Global->{db_classinst_cache} = { };
	# Retrieve the iinstset.
	my $projinst = $Global->{default_projinst};
	my $gp = $Global->{projinst}{$projinst};
	my $constants = $gp->{constants};
	my $iinstset_tablename = $constants->{dxso_iinstsettable};
	my $iinstset_raw =
		$dbh->selectall_arrayref(
			"SELECT * FROM $iinstset_tablename WHERE projinst=" . $dbh->quote($projinst),
			{ Slice => {} })
		|| [ ];
	my $iinstset = process_raw_iinstset($projinst, $iinstset_raw);
	$gp->{iinstset} = $iinstset;

}

sub startup_stage1_dbset {
	my($class) = @_;
	my $do_ping = $Global->{dat}{dxso_startup_ping_all_dbinsts};
	# make a DBSet and store it somewhere appropriate
	my $projinst = $Global->{default_projinst};
	my $dbset = DBIx::ScaleOut::DBSet->new($
}

sub startup_stage1_memcached {
	my($class) = @_;
	# this Cache::Memcached is going to get fork()ed, does its new()
	# set up any connections that this could cause a problem for?
	my $day = $Global->{dat};
	return if !$dat->{dxso_memcached};
	my $servers = $dat->{dxso_memcached_servers};
	my $debug = $dat->{dxso_memcached_debug};
	my $server_ar = [ split / /, $servers ];
	for my $server (@$server_ar) {
		$server = [ $1, $2 ] if $server =~ /^(.+)=(\d+)$/;
	}
	my $memcached = Cache::Memcached->new(
		servers =>	$server_ar,
		debug =>	$debug,
	);
	if (!$memcached) {
		warn "ignoring memcached, cannot connect to '$servers'";
		$memcached = '';
	}
	$Global->{cache}{memcached} = $memcached;
}

sub startup_stage1_cachememory {
	my($class) = @_;
	# presumably fork()ing a Cache::Memory is not a problem
	# XXX keep in mind Brian's in-process memcached idea
	my $projinst = $Global->{projinst};
	my $dat = $Global->{dat};
	# some constants should override these, obviously
	my $cachememory = Cache::Memory->new(
		namespace =>		"dsxo_$projinst",
		removal_strategy =>	'Cache::RemovalStrategy::LRU',
		default_expires =>	'3600 seconds',
		size_limit =>		1024*1024,
	);
	if (!$cachememory) {
		warn "ignoring cachememory, cannot Cache::Memory->new";
		$cachememory = '';
	}
	$Global->{cache}{memory} = $cachememory;
}

sub cache_get {
	my($class, $bits, $key) = @_;
	return undef if !$bits;
	my $val;
	# XXX should be constants. need a DBIx::ScaleOut::Constants?
	if ($bits & 0x1) {
		# check hard perl cache, return if found
		$val = $Global->{cache}{hardperl}{$key};
		return $val if defined $val;
	}
	if ($bits & 0x2 && $Global->{cache}{memory}) {
		# check Cache::Memory, return if found
		# Cache::Memory doesn't handle complex structures automatically.
		$val = $Global->{cache}{memory}->get($key);
		if (defined $val && length $val) {
			# In one expression we can both test the first byte
			# of the retrieved value and clear it (so the
			# remaining bytes will be the original set value).
			if ((substr($val, 0, 1) = '') eq '0') {
				# it's a scalar, return the remainder as-is
			} else {
				# it's a frozen data structure
				$val = Storable::thaw($val);
			}
			return $val;
		}
	}
	if ($bits & 0x4 && $Global->{cache}{memcached}) {
		# check memcached, return if found
		my $val = $Global->{cache}{memcached}->get($key);
		return $val if defined $val;
	}
	return undef;
}

# XXX need a cache_get_multi

sub cache_set {
	my($class, $bits, $expir, $key, $value) = @_;
	return if !$bits;
	if ($bits & 0x1) {
		$Global->{cache}{hardperl}{$key} = $value;
	}
	if ($bits & 0x2 && $Global->{cache}{memory}) {
		# Cache::Memory doesn't handle complex structures automatically.
		if (ref $value) {
			$value = "1" . Storable::nfreeze($value);
		} else {
			$value = "0$value";
		}
		$Global->{cache}{memory}->set($key, $value, $expir);
	}
	if ($bits & 0x4 && $Global->{cache}{memcached}) {
		$Global->{cache}{memcached}->set($key, $value);
	}
}

sub cache_delete {
	my($class, $bits, $key) = @_;
	if ($bits & 0x1) {
		delete $Global->{cache}{hardperl}{$key};
	}
	if ($bits & 0x2) {
		$Global->{cache}{memory}->remove($key);
	}
	if ($bits & 0x4) {
		# XXX a datum to indicate a default memcached
		# deletion time maybe? since it relates to
		# reader replication lag, maybe should be
		# iinst-specific?
		$Global->{cache}{memcached}->delete($key, 0);
	}
}


############################################################
############################################################

sub getConstants {
	my($projinst) = @_;
	$projinst ||= default_projinst();
	my $setup_class = "DBIx::ScaleOut::Setup::$projinst";
	require $setup_class;
	my $constantstable = $setup_class->{constantstable};
	# hit the main shard directly (in a way that doesn't use ::Base)
	# and do a straight key-value extraction on the constants table
	# then apply reasonable defaults
	my $constants = { };
	$constants->{class_default} = 'DBIx::ScaleOut::Base';
	return $constants;
}

=head1 SYNOPSIS

  use DBIx::ScaleOut 'myprojinst';

  $rows = db()->insert('table1', { id => 3, -timecol => 'NOW()' });
  $timecol = db()->selectOne('timecol', 'table1', "id=" . $db->quote($id));
  my $db = db();
  $success = $db->set('user', $uid, { bio => $bio });
  $hr = $db->get('user', $uid);

DBIx::ScaleOut::Base is the base class from which your custom
classes can inherit;  it defines insert(), set(), etc.  A constant
in dxso_constants specifies which base class to use, and you can
set it to your subclass or use the default 'DBIx::ScaleOut::Base'.

db() is often called without arguments and returns the object for
your project's default ::Base class.  Another common invocation
is db('My::Class') which returns the object for that subclass of ::Base.

There is exactly one object created for each ::Base subclass.  Each
such object has-a DBSet object whose primary job is to pick a dbinst.

projinst:	default default 'main'
shard:		default class's default 'main'
purpose:	default class's default 'w'
gen:		generation
weight:		weight within the generation

db()
	function, not method
	calls startup() if !$Global
	
startup()
	

Examples:

projinst	shard		purpose		gen	weight	dbinst

# The main dbset, used by default
main		main		w		0	1	slashdot
main		main		w		1	1	slashdot02
# (the above line means that if the first-choice writer is down, slashdot02 is a fallback writer)
main		main		r		0	2	slashdot02
main		main		r		0	1	reader03
main		main		r		0	1	reader04
main		main		r		1	1	slashdot
# (the above line means that if all slashdot readers are down, the writer is a fallback reader, but otherwise it is not used as a reader)

# The shard used when the writer is switched to slashdot02
main		main02		w		0	1	slashdot02
main		main02		w		1	1	slashdot
# (the above line means that if the first-choice writer is down, slashdot is a fallback writer)
main		main02		r		0	2	slashdot
main		main02		r		0	1	reader03
main		main02		r		0	1	reader04
main		main02		r		1	1	slashdot02
# (the above line means that if all slashdot readers are down, the writer is a fallback reader, but otherwise it is not used as a reader)

# The dbset used for getStoriesEssentials (when the writer is the main writer, anyway)
main		gse		r		0	1	reader03
main		gse		r		0	1	reader04
main		gse		r		1	1	slashdot02

main		search		r		0	1	search
# (no repl=W for shard=search because the code that uses search never needs to write anything)
# (no repl=R, gen=1 for shard=search because no other dbinst has the indexes needed for searching)

main		log		w		0	1	log
main		log		r		0	1	log_reader
# (there's no purpose=r, gen=1, dbinst=log for shard=log because the log reader does not fail over to its writer)

A "shard" may be used for an actual data shard, as in the "log"
example, i.e. a separate database or databases to access separate
data.  Or it may be simply a different perspective on your network
of databases, e.g. you may have replicated slaves set aside for a
particular type of access (backup, specialized reads) and a shard
may define 

A shard may have zero or more writers, though you really don't want
to define more than one.  Writes are sent exclusively to dbinst's with
a "w" purpose, reads are exclusively from dbinst's with an "r" purpose
unless no valid "r"s exist, in which case a "w" is used.  If your shard
will be used exclusively for reading, it's perfectly fine to not define
a "w" purpose dbinst.

The time at which an "r" (and "w" if you are silly and define more
that one) is picked is called reroll() time.  Whenever you send an
insert/update/delete to your "w" dbinst, the tables you touch are
tracked.  Any subsequent attempt to read from any of those tables
will not consider your "r" valid, thus sending the read to your "w"
instead.  However for this (and other) purposes, dxso considers shards
to be disjoint.

Turn off auto-transaction-commit, and while it is off all queries
go to the writer.

If you want to open multiple dbh's to the same dbinst, this will
have to be done manually, by calling connect_nocache_dbinst.  If you
think you need to do this, check whether you can make do with multiple
sth's.  In fact most of the time you don't even need that.

It'd be nice to have a method that means "set the default reader purpose
for shard s to 'foo' until the next reroll", so e.g. admin pages could
set the reader to 'w'.

So the defaults are:
	projinst	default default 'main', default set at create, set at db
	shard		default 'main', set at db
	purpose		default 'reader' for reader, set at db
			'writer' for writer, cannot be changed
	repl		goes away ('writer' weightset is w, all others are r)
			(projinst+shard+purpose defines a dbset)
	gen, weight, dbinst	as before

A reroll clears out any previously-stored writer and reader.

* = typically called by application

* create($projinst) - exported function
	Preferably, called at Apache::ExtUtils::command_table() time,
	i.e. when the parent Apache process is first run (see
	Slash/Apache/Makefile.PL).
	Or, called at the top of a script works fine but is a bit slower:
	adds 2 SELECTs to the main writer and the ping()s in DBSet::new
	for each new apache child process (not each HTTP request).
	In $global:  sets the default projinst;  loads the dxso_constants and
	dxso_purposes for that projinst;  new()s the caches if
	necessary for that projinst;  new()s the DBSet for that projinst.
	Sets the project rollcount to 1.
DBSet::new($projinst) - object constructor
	require's all dbinst .pm files referenced by the projinst.
	Pings the DBs they reference and warns for any not present
	(may take as long as 1 second per dbinst).
* db($class, $shard, $purpose, $opts) - exported function
	A cached new(), usually a fast lookup to return the object
	in cache, but if create() has not initialized the global
	object instance, it does that first.  If the class is custom,
	that class's new() is called (that class is expected to
	have DBIx::ScaleOut in its @ISA and to call SUPER::new).
	Overriding the method 'default_shard' or 'default_purpose'
	gives DB's created in that class a different default shard
	or purpose.  If $opts->{dbh_nocache} (or something) is
	specified, pass that along to new.
new($class, $projinst, $shard, $purpose, $opts) - object constructor
	Just copy the arguments into a hash and bless it.
* $db->selectAllHrAr() - object method
	If the object's r_dbh is false, calls connect() (if that fails,
	returns undef).
	Then does the DBI call on that dbh.
* $db->insert() - object method
	If the object's {readonly} param is true, returns undef;
	if the object's w_dbh is false, calls connect('w') (if that
	fails, returns undef).
	Then does the DBI call on that dbh.
$db->connect() - object method
	Basically the first thing called by all query methods.
	Returns the dbh chosen to be used for the object's purpose,
	or undef.
	Calls DBSet::get with the db object's shard and purpose
	to retrieve the dbinst and its dbh, for both w and r.
	If the writer already exists, don't re-get it.
	But if the object's rollcount < project rollcount, force a
	re-get() of the reader.
	It'd be nice to allow an opt such that, if $db->{dbh_nocache},
	this calls instead
	DBIx::ScaleOut::connect_nocache_dbinst($dbinst)).  But
	see getDB() and reroll() and dbobjcache for issues there
	(need to be able to rollback transactions at reroll time).
connect_cached_dbinst($dbinst) - function
	Calls ${"DBIx::ScaleOut::Setup::${dbinst}::params"} to get
	params and returns the dbh from DBI->connect_cached().
	Can we set a readonly var in DBI for this connection?
	Hmmm what about Apache::DBI, is it going to interfere?
connect_nocache_dbinst($dbinst) - function
	Calls ${"DBIx::ScaleOut::Setup::${dbinst}::params"} to get
	params and returns the dbh from DBI->connect().
	Can we set a readonly var in DBI for this connection?
* reroll($projinst) - class method
	Increments the global rollcount for the project.
	Any db's with any open transactions get a rollback and a
	warn/die.
	Note that Apache::DBI pushes an apache PerlCleanupHandler,
	inside the child, for every unique connection parameter
	(i.e. every unique dbh).  This may be a good idea for DXSO
	too, to prevent the application from having to call this
	manually.

# XXX remember to strip off unnecessary semicolon from end of sql statements

=head1 AUTHOR

Jamie McCarthy <jamie@mccarthy.vg>

=cut

sub init_class_global {
	if (!defined $global) {
		if ($ENV{GATEWAY_INTERFACE} && (my $r = Apache->request)) {
			my $cfg = Apache::ModuleConfig->get($r, 'Slash::Apache');
			$global = $cfg->{dxso_global} ||= {};
		} else {
			$global = {};
		}
	}
}

sub get_class_global { $global }

sub get_constants {
	my($projinst) = @_;
	die "createDB() has not been called"
		if !$global;
	die "createDB() has not been called for projinst '$projinst'"
		if !$global->{projinst}{$projinst};
	return $global->{projinst}{$projinst}{constants};
}

# Basically a cached new().  Objects of a given (projinst, class, shard)
# are all considered to be the same.
# This is in CamelCaps because it's expected to be called frequently
# from outside.
# Of course all objects of a given projinst all share the same dbset
# and caches anyway, regardless of class or shard, so they are
# essentially the same already.
# (Should each shard get separate caches, with a separate namespace?
# hm... maybe)
# getDB() gets everything ready to connect, but does not try to connect.
# The first query sent to it will attempt to connect.
# I'm not entirely happy with the presence of %$opts meaning that no
# caching is done.  "readonly" may be a common enough opt that we
# don't want to re-new() for each one.

#sub db {
#	my($class, $shard, $purpose, $opts) = @_;
#	
#	die "createDB() has not been called"
#		if !$global;
#	$class   ||= 'DBIx::ScaleOut::Base';
#	$shard   ||= $class->default_shard();
#	$purpose ||= $class->default_purpose();
#
#	# opts is the only way to specify a separate project (it would
#	# be rare, which is why it's buried in opts).  If that is the
#	# only opt given, forget about the hashref so the resulting
#	# object may be cached normally.
#	my $projinst;
#	if ($opts && $opts->{projinst}) {
#		$projinst = $opts->{projinst};
#		$opts = undef if !%$opts;
#	}
#	$projinst ||= $global->{default_projinst};
#
#	my $key = join('-', $class, $shard, $purpose);
#	if (!$opts &&  $global->{projinst}{$projinst}{dbobjcache}{$key}) {
#		return $global->{projinst}{$projinst}{dbobjcache}{$key};
#	}
#	my $self = $class->new($projinst, $shard, $purpose, $opts);
#	return undef if !$self;
#	my $dbset = $self->get_dbset();
#	$global->{dbobjcache}{$key} = $self if !$opts;
#	# Note, if $opts is true, $self needs to go into dbobjcache somewhere
#	# so that it can be reroll()'d... and if $opts includes 'nocache',
#	# reroll() needs to purge it from the cache so its memory and
#	# connection handle can be reclaimed.
#	return $self;
#}

# This serves several purposes:
# It ends any open transaction (probably ROLLBACK and warn);
# It notes that one task is complete and so any tables or data object types
# which have been modified no longer are required to be loaded from the
# writer and can be pulled from the reader again;
# It picks (possibly) new dbinst's to connect to when a connection is
# next required;
# It does NOT call DBI->disconnect for any dbinst's.
# Note that if the db's purpose does not include a writer, then its
# w_inst, w_dbh will always be false.
# Note that if I do
#   $stats = getDB('Slash::Stats');
#   $data = getDB('Slash::Data');
#   $stats->insert("table1", { blah });
#   $data->select("table1", { blah });
# the fact of the insert/update/delete to table1 needs to be tracked at the
# shard level

# For reroll, see note on nocache in getDB.
# Class method.

sub reroll {
	my($class, $projinst) = @_;
	$projinst ||= $global->{default_projinst};
	my $dbobjcache = $global->{projinst}{$projinst}{dbobjcache};
	for my $key (sort keys %$dbobjcache) {
		$dbobjcache->{$key}->check_rollback();
	}
	$global->{projinst}{$projinst}{rollcount}++;
}

# obj method in ::DB
sub check_rollback {
	my($self) = @_;
	# Only need to check the writer, obviously
	my $dbh = $self->{w_dbh};
	return unless $dbh;
	if ($dbh->{Active} && !$dbh->{AutoCommit} && eval { $dbh->rollback() }) {
		warn "transaction left open at reroll (and rolled back)"
			. " in project $self->{projinst}, dbinst $self->{w_inst}";
	}
}

# Obsolete. Mine this for good ideas then delete.

sub createDB {
	my($projinst, $not_default) = @_;
	init_class_global();
	$projinst ||= 'main';
	# If this project was already created and initialized,
	# no need to repeat ourselves.
	return if $global->{projinst}{$projinst};

	# maybe set a default shard too?
	# hmmm no I think the default shard should always be 'main'.
	# so for projinst, 'main' is only the default default
	# while for shard, 'main' is the default

	# Because this is obviously called before the db is set up
	# for this projinst, and yet we need some data from its
	# main writer, we here open a raw connection.  Open a
	# raw DBH to the main writer for this projinst, which
	# is always the dbinst with the same name.
	# Only pass in the dbinst (which happens to be $projinst);
	# don't pass in a projinst since the project is not set up.
	my $dbh = connect_cached_dbinst($projinst);
	# Retrieve all its constants first.
	my $constants_tablename = 'dxso_constants';
	my $constants_raw =
		$dbh->selectall_arrayref(
			"SELECT * FROM $constants_tablename WHERE projinst=" . $dbh->quote($projinst),
			{ Slice => {} })
		|| [ ];
	my $constants = process_raw_constants($projinst, $constants_raw);
	$global->{projinst}{$projinst}{constants} = $constants;

	# Retrieve the iinstset.
	my $iinstset_tablename = $constants->{iinstset_tablename};
	my $iinstset_raw =
		$dbh->selectall_arrayref(
			"SELECT * FROM $iinstset_tablename WHERE projinst=" . $dbh->quote($projinst),
			{ Slice => {} })
		|| [ ];
	my $iinstset = process_raw_iinstset($projinst, $iinstset_raw);
	$global->{projinst}{$projinst}{iinstset} = $iinstset;

	# Set up the DBSet and the caches.
	my $dbset = DBIx::ScaleOut::DBSet->new($projinst);
	$global->{projinst}{$projinst}{dbset} = $dbset;
	if ($constants->{memcached_use}) {
		my $memcached = create_memcached_object($projinst, $constants);
###		$global->{projinst}{$projinst}{memcached} = $memcached;
	}
	if ($constants->{cachememory_use}) {
		my $cachememory = create_cachememory_object($projinst, $constants);
###		$global->{projinst}{$projinst}{localcache} = $localcache;
	}

	# Retrieve the vars.  Now that the DBSet and the caches are
	# set up, we can retrieve this data in the usual way.
#	my $vars_raw = getDB()->getKV('objecttype');

	$global->{projinst}{$projinst}{rollcount} = 1;

	$global->{default_projinst} = $projinst unless $not_default;
}

sub process_raw_constants {
	# Convert an arrayref of hashrefs into a hashref.
	my($projinst, $ar) = @_;
	my $constants = { };
	for my $hr (@$ar) {
		my($name, $value) = ($hr->{name}, $hr->{value});
		$constants->{$name} = $value;
	}

	# Set default values for any missing constants.
	$constants->{iinstset_tablename} ||= 'dxso_iinstset';
	$constants->{connect_timeout}    ||= 5;
	# ...others?

	return $constants;
}

sub process_raw_iinstset {
	# Convert an arrayref of hashrefs into a hashref.
	my($projinst, $ar) = @_;
	my $iinstset = { };
	# This consolidation is necessary because (for reasons of
	# performance and simplicity) the dxso_iinstset table is
	# not normalized.  (It's better to perform this little bit
	# of data massaging than to proliferate normalized tables
	# and expect admins to maintain them.)
	for my $hr (@$ar) {
		my(	            $shard, $purpose,    $gen,       $weight, $dbinst) =
			    @$hr{qw( shard   purpose      gen         weight   dbinst )};
			$iinstset->{$shard}{$purpose} ||= [ ];
			$iinstset->{$shard}{$purpose}   [$gen] ||= [                  ];
		push @{ $iinstset->{$shard}{$purpose}   [$gen] },  [ $weight, $dbinst ];
	}
	# If there's no purpose given for the main shard (which will be
	# the case if the dxso_iinstset table is missing since there
	# will be no raw purposes at all), add in a reader and writer
	# pointing that projinst's main shard's main purpose's dbinst
	# to itself.
	for my $purpose (qw( r w )) {
		next if defined $iinstset->{main}{$purpose}[0];
				$iinstset->{main}{$purpose}[0] =   [       1,  'main' ];
	}
	# Normalize weights.
	sub _normalize_weights {
		my($iinstset, $projinst, $shard, $purpose, $gen, $duples) = @_;
		my $weightsum = 0;
		for my $duple (@$duples) { $weightsum += $duple->[0] }
		if (!$weightsum) {
			# This can happen if 'weight's are set
			# to 0 or if 'gen's are skipped. XXX handle better
			die "weightsum=0 for $projinst $shard $purpose $gen";
		}
		for my $duple (@$duples) { $duple->[0] /= $weightsum }
	}
	DBIx::ScaleOut::DBSet::foreach_purpose($iinstset, $projinst, \&_normalize_weights);

	return $iinstset;
}

sub import {
#	die "this class is not part of Exporter (yet). import called with: '@_'";
}

sub handle_fork {
	# See InactiveDestroy in perldoc DBI
}

############################################################

# Object accessor methods

sub get_dbset {
	my($self) = @_;
	return $global->{projinst}{ $self->{projinst} }{dbset};
}

############################################################

# Class utility functions (not methods)

sub connect_cached_dbinst {
	# Can be called before a projinst is set up, so $projinst is optional.
	my($dbinst, $projinst) = @_;
	$projinst ||= '';
	my $params = get_dbinst_params($dbinst);
	my $dbh = undef;
	my $timeout = $projinst
		? get_constants($projinst)->{connect_timeout}
		: 5;
	eval {
		local $SIG{ALRM} = sub { die 'dxso_connection_timeout' };
		# would be nice to have a constant that can specify no use of alarm
		alarm $timeout if $Config{d_alarm};
		$dbh = DBI->connect_cached($params->{connect},
			$params->{dbuser}, $params->{password}, $params->{attributes});
		alarm 0        if $Config{d_alarm};
	};
	if (!$dbh || $@) {
		warn "DBIx::Scaleout connection_failed for '$dbinst' in '$projinst': $DBI::errstr";
		return undef;
	}
	return $dbh;
}

sub connect_nocache_dbinst {
	# to be written (mostly copied from above)
}

sub get_dbinst_params {
	my($dbinst) = @_;
	if (!defined ${"DBIx::ScaleOut::Setup::${dbinst}::params"}) {
		eval { require "DBIx/ScaleOut/Setup/$dbinst.pm" };
		die "require failed for '$dbinst': $@" if $@;
	}
	return ${"DBIx::ScaleOut::Setup::${dbinst}::params"};
}

############################################################

# This would go to DBIx::ScaleOut::DB

# maybe allow an option that specifies r or w connect only?
# maybe someday have a force_disconnect?
#	my($self, $rw, $force_disconnect) = @_;
#	$self->disconnect($rw) if $force_disconnect;

sub connect {
	my($self) = @_;
	my $projinst = $self->{projinst};
	my $dbset = $self->get_dbset();
	my $shard = $self->{shard};
	my $proj_rollcount = $global->{projinst}{$projinst}{rollcount};
	my $reroll = $self->{rollcount} < $proj_rollcount;
	if ($reroll || !$self->{w_dbh} || !$self->{w_dbh}->{Active}) {
		($self->{w_dbh}, $self->{w_dbinst}) = $dbset->get($shard, 'w');
		if (!$self->{w_dbh}) {
			die "DBIx::Scaleout connect_failed for $self->{shard} w w in '$projinst'";
		}
	}
	if ($reroll || !$self->{r_dbh} || !$self->{r_dbh}->{Active}) {
		($self->{r_dbh}, $self->{r_dbinst}) = $dbset->get($shard, $self->{purpose});
		if (!$self->{r_dbh}) {
			die "DBIx::Scaleout connect_failed for $self->{shard} r $self->{purpose} in '$projinst'";
		}
	}
	$self->{rollcount} = $proj_rollcount;
}

# Not sure if this will be needed, at least, not within the class itself maybe.
sub disconnect {
	my($self) = @_;
	my @rw = qw( r w );
	for my $x (@rw) {
		my $dbh = $self->{"${x}_dbh"};
		next unless $dbh;
		$dbh->disconnect;
		# undef/'' the instance variables here I guess
	}
}

# This would go to DBIx::ScaleOut::DB
sub need_writer_for_tables {
	my($self, $tables) = @_;
	my $shard = $self->{shard};
	# Check some projinst/shard-specific list of tables modified.
	return 0;
}

# This would go to DBIx::ScaleOut::DB
sub selectRowA {
	my($self, $cols, $tables, $where, $other, $opts) = @_;

	# warn, if any reference strings in $where

	# This loads w_dbh and r_dbh.
	return undef unless $self->connect();

	# Canonicalize the list of tables.
	# Class methods can be called with $self->, right?
	my $tablelist = $self->canonicalize_tables($tables);
	$tables = $self->tablelist_to_string($tables);
	$other = " $other" if length($other);

	# Use the reader if we can, otherwise the writer.
	my $dbh = $self->need_writer_for_tables($tables)
		? $self->{w_dbh}
		: $self->{r_dbh};

	# querylog start
	# trace start

	my $sql = "SELECT $cols FROM $tables WHERE $where$other";
	my $sth = $dbh->prepare($sql);
	if (!$sth->execute()) {
		$self->log_error($sql);
		return undef;
	}
	my @r = $sth->fetchrow();
	$sth->finish();

	# trace finish
	# querylog finish

	@r;
}

sub select1 {
	my $self = shift;
	my @r = $self->selectRowA(@_);
	return @r ? $r[0] : undef;
}

sub _get_inits { # dumb name, change this
	my($self) = @_;
	my $projinst = $self->{projinst};
	return (
#		$dbset->{$projinst}	|| undef,
#		$cm->{$projinst}	|| undef,
#		$memcached->{$projinst}	|| undef,
	);
}

# Obviously this needs to be smarter.
# I'd kinda like to see 'foo, bar LEFT JOIN baz' canonicalized to
# ('foo', 'bar', 'LEFT JOIN', 'baz') -- it's simple and it might
# be useful enough.  We could create constants for each kind of
# join and try constructing syntax trees, but I don't think that
# will be necessary.

sub canonicalize_tables {
	my($class, $tables) = @_;
	return $tables if ref $tables;
	my @tables = split /\s*,\s*/, $tables;
	return \@tables;
}

sub tablelist_to_string {
	my($class, $tablelist) = @_;
	return join ', ', $tablelist;
}


# These were called by new()
#
#sub populate_class_global {
#	my($projinst) = @_;
#	init_class_global();
#	populate_projinst_global_dbset($projinst);
#	populate_projinst_global_other ($projinst);
#}
#
#sub populate_projinst_global_dbset {
#	my($projinst) = @_;
#	if (!defined($global->{projinst}{$projinst})) {
#		$global->{projinst}{$projinst}{dbset} = DBIx::ScaleOut::DBSet->new($projinst);
#	}
#}
#
#sub populate_projinst_global_other {
#	my($projinst) = @_;
#	if (!defined($global->{projinst}{$projinst})) {
#		$global->{projinst}{$projinst}{cm} =
#			Cache::Memory->new(namespace => "dxso::$projinst::");
#		$global->{projinst}{$projinst}{memcached} =
#			Cache::Memcached->new(namespace => "dxso::$projinst::");
#	}
#}


1;

