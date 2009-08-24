package DBIx::ScaleOut::Setup;

use DBI;
#use Fcntl qw( F_SETFD F_GETFD );
use File::Temp;
use File::Spec;
use Net::Ping;
use Data::Dumper;

use Exporter 'import';
@EXPORT = qw( edit );

sub SETUPFILENAME () { '_setupfile.pm' }

sub new {
	my($class, $init) = @_;
	$init ||= { };
	my $self = bless $init, $class;
	return $self;
}

# FUNCTION, NOT METHOD
# Intended to be called by e.g.:
# perl -MDBIx::ScaleOut::Setup -e edit
# after module is already installed

# See note before Module::Build->subclass call in Build.PL

sub edit {
	my $setup = DBIx::ScaleOut::Setup->new();
	my($configtext, $dbinsts_hr) = $setup->do_edit();
	
}

# do_edit does all the work, loading current values if any, defaults if
# not, querying for editor, looping until valid, testing connection(s)
# for validity, unix read/write permissions, etc.  If $prompt_callback
# is set, calls that function to print prompts and get user feedback,
# otherwise handles that itself.
# It returns a hashref with the data the user ended up with.  (Former
# versions took it upon themselves to write the data into the currently
# installed DBIx/ScaleOut/Setup.pm and .../Setup/*.pm files, but we're
# not sure how we want to do that right now.)
sub do_edit {
	my($self, $prompt_callback) = @_;

	sub _edit_default_prompt_callback {
		require Module::Build::Base;
		# This is pretty lame -- its first argument is a
		# Module::Build::Base object but we don't want to
		# build one so I'm just passing undef which works
		# now but it's not a stable solution... would be
		# better to copy/paste code from M:B:B:p...
		# also, I don't like how it prints the result
		#
		# 2009-08: I don't think undef does work, not sure
		# why I thought it did.  Use which editor? [/usr/bin/vi ]Can't
		# call method "_is_unattended" on an undefined
		# value at /usr/local/lib/perl5/5.10.0/Module/Build/Base.pm
		# line 532, <DATA> line 644.
		my($self, $prompt, $def) = @_;
		return Module::Build::Base::prompt(undef, $prompt, $def);
	}

	$prompt_callback ||= \&_edit_default_prompt_callback;
	my $intro_text = $self->get_intro_text();
	my $prompt_text = $self->get_prompt_text();
	my $invalid_exec_text = $self->get_invalid_exec_text();
	my $retry_text = $self->get_retry_text();
	my $default_editor = $self->get_default_editor();

	# XXX Should check this text's validity and matching with loadable modules
	my $setupfile_text = $self->get_setupfile_text();

	my $first_time_thru = 1;
	my $dbinst_hr = { };
	while (!%$dbinst_hr) {
		my $editor = '';
		while (!$editor) {
			if ($first_time_thru) {
				print $intro_text;
				$first_time_thru = 0;
			}
			$editor = &$prompt_callback(
				$self, $prompt_text, $default_editor);
			if (!$self->is_acceptable_editor($editor)) {
				print $invalid_exec_text;
				$editor = '';
			}
		}
		$setupfile_text = $self->get_edited_text($setupfile_text, $editor);
		my $err_ar;
		($dbinst_hr, $err_ar) = $self->parse_setupfile_text($setupfile_text);
#print STDERR "do_edit dbinst_hr: " . Dumper($dbinst_hr);
		if (!@$err_ar) {
			$err_ar = $self->check_dbinsts($dbinst_hr);
		}
		if (@$err_ar) {
			$dbinst_hr = { };
			$self->print_err($err_ar);
			print $retry_text;
		}
	}
	return($setupfile_text, $dbinst_hr);
}

sub write_files {
	my($text, $dbinst_ar) = @_;
	my $dir = get_setup_dir();
	
		# Emit $setupfile_text to $INC{DBIx/ScaleOut/Setup/_setupfile.pm}
		# (some scalar to Dumper dump the data)
		# Emit $dbinst_hr to $INC{DBIx/ScaleOut/Access/$dbinst.pm}.
		# (it has to define dbinsts() which returns a hashref of dbinsts)
		# Its $dbinsts arrayref contains the data.
		# Don't forget to insert a comment as first line
	# put together a {dsn} field for each dbinst -- no, probably construct this when used
	# and assign any blank fields their defaults, incl.: port, constantstable
	# (if socket or hostname+port are blank, leave blank because the other is used, of course)

	# currently this is all done in D:S:ModuleBuild::ACTION_install
	# and needs to be moved here
}

sub get_intro_text {
	my($self) = @_;
	return <<EOT;

Welcome to DBIx::ScaleOut.  A short setup procedure is required
to allow this perl module to communicate with a database instance.
We'll run through that right now.  The first step is for you to pick
your favorite text editor so you can edit a text file specifying your DB.
(There will be more instructions in that file, in a moment.)

EOT
}

sub get_prompt_text {
	my($self) = @_;
	return "Use which editor?";
}

sub get_invalid_exec_text {
	my($self) = @_;
	return "Not executable.\n";
}

sub get_retry_text {
	my($self) = @_;
	return "Please continue editing.\n\n";
}

sub get_default_editor {
	my($self) = @_;
	my $editor = $ENV{EDITOR} || '';
	$editor = '/etc/alternatives/vi'	if !$self->is_acceptable_editor($editor);
	$editor = `which vi`, chomp $editor	if !$self->is_acceptable_editor($editor);
	$editor = ''				if !$self->is_acceptable_editor($editor);
	return $editor;
}

sub is_acceptable_editor {
	my($self, $editor) = @_;
	return 1 if $editor && -x $editor;
}

sub get_setupfile_text {
	my $text = get_setupfile_text_require();
	$text  ||= get_setupfile_text_default();
	return $text;
}

sub get_setupfile_text_require {
	my $filename = File::Spec->catfile('DBIx', 'ScaleOut', 'Setup', SETUPFILENAME);
	eval { require $filename; };
	return $@ ? '' : $DBIx::ScaleOut::Setup::setupfile;
}

sub get_setupfile_text_default {
	my($root_user, $root_gid) = (getpwuid(0))[0,3];
	my($root_group) = (getgrgid($root_gid))[0];
	return <<"EOF";
##############################
# Hello,
#
# This is the file you need to edit to configure the DBIx::ScaleOut
# perl module.
# 
# Each dbinst that you configure will be a set of variables that
# instruct your code how to connect to one database instance.
# You'll need to configure at least one, or DBIx::ScaleOut will
# serve no purpose.  Each must begin with a "dbinst=" line
# and then the rest of the lines for that dbinst can appear in
# any order.  After you fill in the values, keep reading, there
# are more instructions further down in this file.
#
# Edit the "#" comments in this file however you want --
# they will be kept and will serve as documentation for the next
# person (perhaps you!) who edits this file.
#
# You probably do want to go through this configuration process
# now, but if you think you want to skip it and come back later,
# put the single word "SKIP" on a blank line anywhere. XXX respect SKIP
##############################

# The first line in each dbinst declaration must be the name of
# the dbinst.  This is the string you'll use in your code to
# identify which database you want to access.  It must be a single
# short word (must match /^[A-Za-z]\\w{0,11}\\\$/, and by convention
# these names are lowercase).  The default default is "main" so
# it's most convenient if you have that one, at least, defined.
dbinst=main
# The config file for this dbinst will provide full database
# access to any unix user that can read it.  Authorization and
# security thus depend on your setting up unix permissions.
# Whatever user/group your webserver runs as is probably correct
# here. XXX note that we need to be to chown to this, so
# 'make install' will need to be run as root or as this user
unixuser=$root_user
unixgroup=$root_group
# Currently the only driver supported is 'mysql'.  (Planning on
# supporting drizzle and postgres eventually.)
driver=mysql
# Identify the host machine the database is on, preferably by IP
# number (IP name may be more convenient but is slightly less
# secure as it enables one more avenue for attack, but it's up
# to you).  "localhost" works too.
host=localhost
# The default port for MySQL is 3306, Drizzle is 4427,
# Postgres is 5432.  Leave blank for your driver's default.
port=
# If you prefer a unix-domain socket to a TCP host:port, set the
# host to blank and specify here your unix socket file.  This may
# look like e.g. "/var/run/mysqld5.0/mysql.sock".
socket=
# This is the username your client will log into the DB with.
dbuser=jamie
# And the password (or blank for none).  Everything between the /=\\s*/
# and /\\n/ is the password, so no need to quote special characters.
# Thus, there's no way to have a newline or any leading whitespace in
# your password.  The password will be stored as plaintext:  there's
# no real way around this.  Storing this password effectively punts
# database authentication down from mysql to unix -- that's a
# DBIx::ScaleOut feature -- so if you have concerns, make sure your
# unixuser/unixgroup above are correct.
password=r8djoqw6
# The name of the database you'll be accessing.
database=wow
# The name of the constants table in that database, which stores
# values that are necessary for the initial stages of setup and
# which rarely change.  Default is 'dxso_constants'.  (If at runtime
# this table is not present, defaults will be used instead.)
constantstable=
# DBI connect attributes go here, "k1=v1;k2=v2", but you won't usually
# need any.
attributes=
# The initial dbinst is the one that DBIx::ScaleOut connects to,
# to obtain some information necessary to initialize itself.
# Typically this would be your "main" dbinst, your writer.
# Don't define more than one dbinst as initial.
initial=1

##############################
# To create additional dbinsts, simply copy and paste the above
# block down here as many times as you want, then edit.  Each
# dbinst must be different of course.
#
# You're probably editing this file right now thanks to
# DBIx::ScaleOut::Setup.  So when you save it, DBIx::ScaleOut::Setup
# will parse it, let you know about any syntax errors, try to contact
# the database(s) you've listed above and let you know about
# connection errors, and give you the opportunity to re-edit it
# as many times as you want.  When you're done, DBIx::ScaleOut::Setup will
# save the parsed data from this file into a DBIx/ScaleOut/Setup/
# directory somewhere in your \@INC.  Once DBIx::ScaleOut is installed,
# you can re-edit this file at any time, and `perldoc DBIx::ScaleOut` will
# have information on doing that.
##############################
EOF
}

sub get_setup_dir {
	my $inc = $INC{'DBIx/ScaleOut/Setup.pm'};
	die "apparently this very module has not been included?" if !$inc;
	my($volume, $dir, $file) = File::Spec->splitpath($inc);
	return $dir;
}

sub get_edited_text {
	my($self, $text, $editor) = @_;
	my $tmpfh = new File::Temp(UNLINK => 1, SUFFIX => '.txt');
	print $tmpfh $text;
	$tmpfh->flush();
	my $filename = $tmpfh->filename;
	close $tmpfh;
	system $editor, $filename;
	my $new = '';
	if (open(my $fh, $filename)) {
		local $/ = undef;
		$new = <$fh>;
		close $fh;
	}
	return $new;
}

# there's probably some module that will do most of this.
# it would also be nice to actually parse it line by line
# so we can pass along line numbers to the next method and
# report line numbers for errors
sub parse_setupfile_text {
	my($self, $text) = @_;
	my $err_ar = [ ];
	$text =~ s/^(?<!\\)#.*//gm;
	$text =~ s/[\r\n]+/\n/g;
	$text =~ s/^\s+//gm;
	chomp $text;
	push @$err_ar, 'No text' if !$text;
	my @tuples = ( );
	if (!@$err_ar) {
		my @lines = split /\n/, $text;
		LINE: for my $line (@lines) {
			next unless $line;
			my($name, $value) = $line =~ /^(\w+)\s*=\s*(.*)$/;
			if (!$name) {
				push @$err_ar, "Invalid line: '$line'";
				last LINE;
			}
#print "tuple found: name=$name value=$value for line: $line\n";
			push @tuples, [ $name, $value ];
		}
	}
	push @$err_ar, 'No tuples' if !@tuples;
#print "err_ar: '@$err_ar'\n";
	return(undef, $err_ar) if @$err_ar;
	($dbinst_hr, $err_ar) = $self->group_tuples(@tuples);
	return($dbinst_hr, $err_ar);
}

sub group_tuples {
	my($self, @tuples) = @_;

	# XXX this hash should be elsewhere
	# XXX store the regex strings pre-qr{} and emit them in get_setupfile_text_default (DRY principle)
	my %field = (
		dbinst		=> {	regex =>	qr{^[A-Za-z]\w{0,11}$}	},
		unixuser	=> {	regex =>	qr{^[a-z]\w{0,15}$}	},
		unixgroup	=> {	regex =>	qr{^[a-z]\w{0,15}$}	},
		driver		=> {	regex =>	qr{^(mysql)$}		},
		host		=> {	regex =>	qr{.?}			},
		port		=> {	regex =>	qr{^\d*$}		},
		socket		=> {	regex =>	qr{.?}			},
		dbuser		=> {	regex =>	qr{.}			},
		password	=> {	regex =>	qr{.?}			},
		database	=> {	regex =>	qr{.}			},
		constantstable	=> {	regex =>	qr{^([a-z]\w{0,31}|)$}	},
		attributes	=> {	regex =>	qr{.?}			},
		initial		=> {	regex =>	qr{^[01]?$}		},
		# XXX I'm pretty sure if 'initial' is kept as a
		# field, we need to allow multiple dbinsts to have
		# a chain of them, go to successive dbinsts on startup
		# if the first one is down.  (That can't be handled
		# at the iinst level because that information is
		# loaded from the initial dbinst.)  This whole "initial"
		# thing is a fussy way of providing a backup/alternative
		# way of specifying the default dbinst for project,
		# and (on the other hand) making the default have the
		# same name (no failover) is probably good enough for
		# almost all cases and has the advantage of simplicity --
		# so let's consider dropping initial entirely.
	);
	my @fields = keys %field;

	my $err_ar = [ ];
	# Verify that every field present is known,
	# and every present field is valid.
	my %dbinst = ( );
	my $cur_dbinst = { };
	my $cur_dbinst_name = '';
	for my $kv (@tuples) {
		my($key, $value) = @$kv;
		if (!$field{$key}) {
			push @$err_ar, "unknown field name '$key'";
			next;
		}
		if ($value !~ /$field{$key}{regex}/) {
			push @$err_ar, "value '$value' (for key $key) does not match regex '$field{$key}{regex}'";
			next;
		}
		if ($key eq 'dbinst') {
			# New dbinst.  If we had an old dbinst, store it
			# in the list.
			my $old_dbinst_name = $cur_dbinst_name;
			$cur_dbinst_name = $value;
			if (%$cur_dbinst) {
				$dbinst{$old_dbinst_name} = $cur_dbinst;
				$cur_dbinst = { };
			}
		} elsif (!$cur_dbinst_name) {
			push @$err_ar, "key $key encountered before any valid dbinst line";
			next;
		}
		$cur_dbinst->{$key} = $value;
	}
	$dbinst{$cur_dbinst_name} = $cur_dbinst if %$cur_dbinst;
	# Verify every field known is present.
	for my $dbinst_name (sort keys %dbinst) {
		my $dbinst = $dbinst{$dbinst_name};
		my @missing = sort
			grep { $_ ne 'initial' } # it's OK to omit the 'initial' field
			grep { !exists $dbinst->{$_} } @fields;
		if (@missing) {
			push @$err_ar, "dbinst '$dbinst' missing fields: '@missing'";
		}
	}
	# Verify exactly one dbinst has 'initial' set.
	my @initials = ( );
	for my $dbinst_name (sort keys %dbinst) {
		for my $field (keys %{$dbinst{$dbinst_name}}) {
			push @initials, $dbinst_name if $field eq 'initial';
		}
	}
	if (scalar(@initials) != 1) {
		push @$err_ar, "more than one dbinst marked as initial: '@initials'";
	}
	# Set defaults.
	if (!@$err_ar) {
		for my $dbinst_name (sort keys %dbinst) {
			my $this_dbinst = $dbinst{$dbinst_name};
			if (!$this_dbinst->{port} && $this_dbinst->{host} && !$this_dbinst->{socket}) {
				# Only set default port if TCP sockets are intended,
				# i.e. if the host is specified and a socket file
				# is not.
				$this_dbinst->{port} =
					  $this_dbinst->{driver} eq 'mysql' ? 3306
					: $this_dbinst->{driver} eq 'drizzle' ? 4427
					: $this_dbinst->{driver} eq 'postgres' ? 5432
					: '';
				if (!$this_dbinst->{port}) {
					push @$err_ar, "port must be specified for driver '$this_dbinst->{driver}', no known default";
				}
			}
			if (!$this_dbinst->{constantstable}) {
				$this_dbinst->{constantstable} = 'dxso_constants';
			}
		}
	}
	%dbinst = ( ) if @$err_ar;
	return(\%dbinst, $err_ar);
}

sub check_dbinsts {
	my($self, $dbinst_hr) = @_;
	my $err_ar = [ ];
	for my $dbinst_name (keys %$dbinst_hr) {
		my $dbinst = $dbinst_hr->{$dbinst_name};
		my $dbh;
		if (!$self->check_host_ping($dbinst)) {
			push @$err_ar, "dbinst $dbinst_name: cannot ICMP ping $dbinst->{host}";
		} elsif (!$self->check_tcp_socket_connect($dbinst)) {
			push @$err_ar, "dbinst $dbinst_name: cannot open TCP connection to '$dbinst->{host}:$dbinst->{port}'";
		} elsif (!$self->check_unix_socket_connect($dbinst)) {
			push @$err_ar, "dbinst $dbinst_name: cannot connect to unix socket at '$dbinst->{socket}'";
		} elsif (!($dbh = $self->check_db_connect($dbinst))) {
			push @$err_ar, "dbinst $dbinst_name: cannot connect to db for dbinst $dbinst->{dbinst}, reported error: '" . $DBI::errstr . "'";
		} elsif (!$self->check_db_ping($dbh)) {
			push @$err_ar, "dbinst $dbinst_name: cannot DBI->ping for dbinst $dbinst->{dbinst}, reported error: '" . $DBI::errstr . "'";
		} else {
			my($ok, $errstr) = $self->check_db_select($dbh);
			if (!$ok) {
				push @$err_ar, "dbinst $dbinst_name: cannot perform SELECT for dbinst $dbinst->{dbinst}, reported error: '$errstr'";
			}
		}
	}
	return $err_ar;
}

sub check_host_ping {
	my($self, $dbinst) = @_;
	my $host = $dbinst->{host};
	my $p = Net::Ping->new();
	return $p->ping($host);
}

sub check_tcp_socket_connect {
	my($self, $dbinst) = @_;
	my($host, $port) = ($dbinst->{host}, $dbinst->{port});
	return 1 if !$host && $dbinst->{socket}; # if using unix sockets, skip this test
	# XXX default port to the driver default here
	my $p = Net::Ping->new("tcp"); # default timeout 5 seconds
	$p->{port_num} = $port;
	$p->service_check(1);
	return $p->ping($host);
}

sub check_unix_socket_connect {
	my($self, $dbinst) = @_;
	my($socket) = ($dbinst->{socket});
	return 1 if !$socket && $dbinst->{host}; # if using tcp sockets, skip this test
	# XXX write test here
	return 1;
}

sub check_db_connect {
	my($self, $dbinst) = @_;
	# obviously building this string should be a function elsewhere in DBIx::ScaleOut
	my $connect_string = "DBI:$dbinst->{driver}:database=$dbinst->{database};host=$dbinst->{hostname}";
	$connect_string .= ";port=$dbinst->{port}" if $dbinst->{port};
	my $attr = { ( map { ($1, $2) } grep { /^([^=]+)=(.*)$/ } split / /, $dbinst->{attributes} ) };
#print STDERR "calling DBI->connect '$connect_string' $dbinst->{dbuser}, $dbinst->{password}, $dbinst->{attributes} attr: " . Dumper($attr);
	my $dbh = DBI->connect($connect_string,
		$dbinst->{dbuser}, $dbinst->{password}, $attr);
	return $dbh || '';
}

sub check_db_ping {
	my($self, $dbh) = @_;
	return $dbh->ping ? $dbh : '';
}

sub check_db_select {
	my($self, $dbh) = @_;
	my($ok, $errstr) = ('', '');
	my $driver = $dbh->{Driver}{Name}; # "The only recommended use for" ->{Driver}
	if (!$dbh) {
		$errstr = $DBI::errstr;
	} elsif ($driver eq 'mysql') {
		# dbs besides mysql are going to have different ways to do this, right?
		# maybe we need a DBIx::ScaleOut::Driver::$foo::check_select() method
		$ok = $dbh->do("SELECT VERSION()");
		$errstr = $ok ? '' : $dbh->errstr;
	}
	return($ok, $errstr);
}

sub print_err {
	my($self, $err_ar) = @_;
	print <<EOT;

Unfortunately, one or more errors were encountered while parsing the
text you just edited.  Here's the list:

EOT
	for my $err (@$err_ar) {
		print "\t$err\n";
	}
	print "\n";
}

1;

