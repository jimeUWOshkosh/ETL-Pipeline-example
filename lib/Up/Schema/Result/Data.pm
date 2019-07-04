use utf8;

package Up::Schema::Result::Data;

# Created by DBIx::Class::Schema::Loader
# DO NOT MODIFY THE FIRST PART OF THIS FILE

=head1 NAME

Up::Schema::Result::Data

=cut

use strict;
use warnings;

use base 'DBIx::Class::Core';

=head1 COMPONENTS LOADED

=over 4

=item * L<DBIx::Class::InflateColumn::DateTime>

=back

=cut

__PACKAGE__->load_components("InflateColumn::DateTime");

=head1 TABLE: C<data>

=cut

__PACKAGE__->table("data");

=head1 ACCESSORS

=head2 data_id

  data_type: 'integer'
  is_auto_increment: 1
  is_nullable: 0

=head2 file_id

  data_type: 'integer'
  is_foreign_key: 1
  is_nullable: 0

=head2 sheet_id

  data_type: 'integer'
  is_foreign_key: 1
  is_nullable: 0

=head2 row_indx

  data_type: 'integer'
  is_nullable: 0

=head2 name

  data_type: 'text'
  is_nullable: 0

=head2 age

  data_type: 'integer'
  is_nullable: 0

=head2 utf

  data_type: 'text'
  is_nullable: 0

=cut

__PACKAGE__->add_columns(
    "data_id",
    { data_type => "integer", is_auto_increment => 1, is_nullable => 0 },
    "file_id",
    { data_type => "integer", is_foreign_key => 1, is_nullable => 0 },
    "sheet_id",
    { data_type => "integer", is_foreign_key => 1, is_nullable => 0 },
    "row_indx",
    { data_type => "integer", is_nullable => 0 },
    "name",
    { data_type => "text", is_nullable => 0 },
    "age",
    { data_type => "integer", is_nullable => 0 },
    "utf",
    { data_type => "text", is_nullable => 0 },
);

=head1 PRIMARY KEY

=over 4

=item * L</data_id>

=back

=cut

__PACKAGE__->set_primary_key("data_id");

=head1 UNIQUE CONSTRAINTS

=head2 C<file_id_sheet_id_row_indx_unique>

=over 4

=item * L</file_id>

=item * L</sheet_id>

=item * L</row_indx>

=back

=cut

__PACKAGE__->add_unique_constraint(
    "file_id_sheet_id_row_indx_unique",
    [ "file_id", "sheet_id", "row_indx" ],
);

=head1 RELATIONS

=head2 file

Type: belongs_to

Related object: L<Up::Schema::Result::Dataset>

=cut

__PACKAGE__->belongs_to(
    "file",
    "Up::Schema::Result::Dataset",
    { file_id       => "file_id" },
    { is_deferrable => 0, on_delete => "NO ACTION", on_update => "NO ACTION" },
);

=head2 sheet

Type: belongs_to

Related object: L<Up::Schema::Result::Datasheet>

=cut

__PACKAGE__->belongs_to(
    "sheet",
    "Up::Schema::Result::Datasheet",
    { sheet_id      => "sheet_id" },
    { is_deferrable => 0, on_delete => "NO ACTION", on_update => "NO ACTION" },
);

# Created by DBIx::Class::Schema::Loader v0.07049 @ 2019-06-16 01:52:44
# DO NOT MODIFY THIS OR ANYTHING ABOVE! md5sum:NDKcOlJQlYtB3OdJUTMWsQ

# You can replace this text with custom code or comments, and it will be preserved on regeneration
1;
