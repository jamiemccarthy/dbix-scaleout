package DBIx::ScaleOut::Setup;

#use DBIx::ScaleOut;
use DBI;
use File::Spec;
use Net::Ping;
use Data::Dumper;

use fields qw( prompt_callback only_return );

use constant SETUPFILENAME => '_setupfile.txt';

sub new {
	my($class, $init) = @_;
	$init ||= { };
	my $self = bless $init, $class;
	return $self;
}

# FUNCTION, NOT METHOD
# Intended to be called by e.g.:
# perl -MDBIx::ScaleOut::Setup -e edit

sub edit {
	my $setup = DBIx::ScaleOut::Setup->new();
	$setup->do_edit();
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
	my @dbinsts = ( );
	while (!@dbinsts) {
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
		($err_ar, @dbinsts) = $self->parse_setupfile_text($setupfile_text);
		if (!@$err_ar) {
			$err_ar = $self->check_dbinsts(@dbinsts);
		}
		if (@$err_ar) {
			@dbinsts = ( );
			$self->print_err($err_ar);
			print $retry_text;
		}
	}
use Data::Dumper; print STDERR "do_edit dbinsts: " . Dumper(\@dbinsts);
	return($setupfile_text, \@dbinsts);
}

sub write_files {
	my($text, $dbinst_ar) = @_;
	my $dir = get_setup_dir();
	
		# Emit $setupfile_text to $INC{DBIx/ScaleOut/Setup/something.pm}
		# (some scalar to Dumper dump the data)
		# Emit @dbinsts to $INC{DBIx/ScaleOut/Setup/$dbinst.pm}.
		# Its $dbinsts arrayref contains the data.
		# Don't forget to insert a comment as first line
	# put together a {dsn} field for each dbinst
	# and assign any blank fields their defaults, incl.: port, constantstable
	# (if socket or hostname+port are blank, leave blank because the other is used, of course)
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
	my $filename = 'DBIx/ScaleOut/Setup/' . SETUPFILENAME;
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
# short word (must match /^[A-Za-z]\\w{0,11}\\$/, and by convention
# these names are lowercase).
dbinst=
# The config file for this dbinst will provide full database
# access to any unix user that can read it.  Authorization and
# security thus depend on your setting up unix permissions.
# Whatever user/group your webserver runs as is probably correct
# here. XXX note that we need to be to chown to this, so
# 'make install' will need to be run as root or as this user
unixuser=$root_user
unixgroup=$root_group
# Currently the only driver supported is 'mysql'.  (Planning on
# supporting more...)
driver=mysql
# Identify the host machine the database is on, preferably by IP
# number (IP name may be more convenient but is slightly less
# secure as it enables one more avenue for attack, but it's up
# to you).  "localhost" works too.
host=localhost
# The default port for MySQL is 3306, Postgres is 5432.  Leave
# blank for your driver's default.
port=
# If host is blank, you can specify a unix socket file.  This may
# look like e.g. /var/run/mysqld5.0/mysql.sock.
socket=
# This is the username your client will log into the DB with.
dbuser=root
# And the password (or blank for none).  Everything between the /=\\s*/
# and /\\n/ is the password, so no need to quote special characters.
# (There's no way to have a newline or any leading whitespace in your
# password.)
password=
# The name of the database you'll be accessing.
database=
# The name of the constants table in that database, which stores
# values that are necessary for the initial stages of setup and
# which rarely change.  Default is 'dxso_constants'.  (If this
# table is not present, of course, defaults will be used instead.)
constantstable=
# Attributes go here, "k1=v1 k2=v2", but you won't usually have any.
attributes=

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
# directory somewhere in your @INC.  Once DBIx::ScaleOut is installed,
# you can re-edit this file at any time, and `perldoc DBIx::ScaleOut` will
# have information on doing that.
#
# Whichever dbinst you put first is the one that DBIx::ScaleOut will
# initially connect to, to retrieve additional data during initialization.
# (XXX how to handle failover? maybe a 'connectorder' field instead?)
# (a field in a dbinst lets us keep the data flat, otherwise we have to
# have a separate 'dbinst connect try order' field and put the dbinsts
# into an arrayref field)
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
	my $tmp = new File::Temp(UNLINK => 1, SUFFIX => '.txt');
	print $tmp $text;
	system $editor, $tmp;
	seek($tmp, 0, 0);
	my $new = '';
	while (my $line = <$tmp>) {
		$new .= $line;
	}
	close $tmp;
	unlink $tmp;
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
			push @tuples, [ $name, $value ];
		}
	}
	push @$err_ar, 'No tuples' if !@tuples;
	return $err_ar if @$err_ar;
	($err_ar, @dbinsts) = $self->group_tuples(@tuples);
	return($err_ar, @dbinsts);
}

sub group_tuples {
	my($self, @tuples) = @_;

	# XXX this hash should be elsewhere
	my %field = (
		dbinst		=> {	regex =>	qr{^[A-Za-z]\w{0,11}$}	},
		unixuser	=> {	regex =>	qr{^[a-z]\w{0,15}$}	},
		unixgroup	=> {	regex =>	qr{^[a-z]\w{0,15}$}	},
		driver		=> {	regex =>	qr{^(mysql)$}		},
		host		=> {	regex =>	qr{.?}			},
		port		=> {	regex =>	qr{^(\d+)?$}		},
		socket		=> {	regex =>	qr{.?}			},
		dbuser		=> {	regex =>	qr{.}			},
		password	=> {	regex =>	qr{.?}			},
		database	=> {	regex =>	qr{.}			},
		constantstable	=> {	regex =>	qr{^[a-z]\w{0,31}$}	},
		attributes	=> {	regex =>	qr{.?}			},
		# if we do a connectorder, check it here
	);
	my @fields = keys %field;

	my $err_ar = [ ];
	# Verify that every field present is known, every field known
	# is present, and every present field is valid.
	my @dbinsts = ( );
	my $cur_user = { };
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
			# New user.  If we had an old user, push it onto
			# the list.
			if (%$cur_user) {
				push @dbinsts, $cur_user;
				$cur_user = { };
			}
		}
		$cur_user->{$key} = $value;
	}
	push @dbinsts, $cur_user if %$cur_user;
	for my $u (@dbinst) {
		my @missing = sort grep { !exists $u->{$_} } @fields;
		if (@missing) {
			push @$err_ar, "dbinst '$dbinst' missing fields: '@missing'";
		}
	}
	@dbinsts = ( ) if @$err_ar;
	return($err_ar, @dbinsts);
}

sub check_dbinsts {
	my($self, @dbinsts) = @_;
	my $err_ar = [ ];
	for my $u (@dbinsts) {
		my $dbh;
		if (!$self->check_host_ping($u)) {
			push @$err_ar, "cannot ICMP ping $u->{host}";
		} elsif (!$self->check_tcp_socket_connect($u)) {
			push @$err_ar, "cannot open TCP connection to $u->{host}:$u->{port}";
		} elsif (!$self->check_unix_socket_connect($u)) {
			push @$err_ar, "cannot connect to unix socket at $u->{socket}";
		} elsif (!($dbh = $self->check_db_connect($u))) {
			push @$err_ar, "cannot connect to db and DBI->ping for dbinst $u->{dbinst}, reported error: '" . $DBI::errstr . "'";
		} else {
			my($ok, $errstr) = $self->check_db_select($dbh);
			if (!$ok) {
				push @$err_ar, "cannot perform SELECT for dbinst $u->{dbinst}, reported error: '$errstr'";
			}
		}
	}
	return $err_ar;
}

sub check_host_ping {
	my($self, $u) = @_;
	my $host = $u->{host};
	my $p = Net::Ping->new();
	return $p->ping($host);
}

sub check_tcp_socket_connect {
	my($self, $u) = @_;
	my($host, $port) = ($u->{host}, $u->{port});
	return 1 if !$host && $u->{socket}; # if using unix sockets, skip this test
	# XXX default port to the driver default here
	my $p = Net::Ping->new("tcp"); # default timeout 5 seconds
	$p->{port_num} = $port;
	$p->service_check(1);
	return $p->ping($host);
}

sub check_unix_socket_connect {
	my($self, $u) = @_;
	my($socket) = ($u->{socket});
	return 1 if !$socket && $u->{host}; # if using tcp sockets, skip this test
	# XXX write test here
	return 1;
}

sub check_db_connect {
	my($self, $u) = @_;
	# obviously building this string should be a function in DBIx::ScaleOut itself
	my $connect_string = "DBI:$u->{driver}:database=$u->{database};host=$u->{hostname}";
	$connect_string .= ";port=$u->{port}" if $u->{port};
	my $attr = { ( map { ($1, $2) } grep { /^([^=]+)=(.*)$/ } split / /, $u->{attributes} ) };
print STDERR "calling DBI->connect '$connect_string' $u->{dbuser}, $u->{password}, $u->{attributes} attr: " . Dumper($attr);
	my $dbh = DBI->connect($connect_string,
		$u->{dbuser}, $u->{password}, $attr);
	return '' if !$dbh;
	return $dbh->ping ? $dbh : '';
}

sub check_db_select {
	my($self, $dbh) = @_;
	my($ok, $errstr) = ('', '(unknown error)');
	if (!$dbh) {
		$errstr = $DBI::errstr;
	} elsif ($u->{driver} eq 'mysql') { # no, test $dbh->{Driver} ... -> something?
		# dbs besides mysql are going to have different ways to do this, right?
		# maybe we need a DBIx::ScaleOut::Driver::$foo::check_select() method
print STDERR "calling do SELECT\n";
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

