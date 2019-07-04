use utf8;

package Up::Schema::Result::Datasheet;

# Created by DBIx::Class::Schema::Loader
# DO NOT MODIFY THE FIRST PART OF THIS FILE

=head1 NAME

Up::Schema::Result::Datasheet

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

=head1 TABLE: C<datasheet>

=cut

__PACKAGE__->table("datasheet");

=head1 ACCESSORS

=head2 sheet_id

  data_type: 'integer'
  is_auto_increment: 1
  is_nullable: 0

=head2 file_id

  data_type: 'integer'
  is_foreign_key: 1
  is_nullable: 0

=head2 sheet_indx

  data_type: 'integer'
  is_nullable: 0

=head2 sheet_name

  data_type: 'text'
  is_nullable: 0

=cut

__PACKAGE__->add_columns(
    "sheet_id",
    {data_type => "integer", is_auto_increment => 1, is_nullable => 0},
    "file_id",
    {data_type => "integer", is_foreign_key => 1, is_nullable => 0},
    "sheet_indx",
    {data_type => "integer", is_nullable => 0},
    "sheet_name",
    {data_type => "text", is_nullable => 0},
);

=head1 PRIMARY KEY

=over 4

=item * L</sheet_id>

=back

=cut

__PACKAGE__->set_primary_key("sheet_id");

=head1 UNIQUE CONSTRAINTS

=head2 C<file_id_sheet_indx_unique>

=over 4

=item * L</file_id>

=item * L</sheet_indx>

=back

=cut

__PACKAGE__->add_unique_constraint(
    "file_id_sheet_indx_unique",
    ["file_id", "sheet_indx"]
);

=head1 RELATIONS

=head2 datas

Type: has_many

Related object: L<Up::Schema::Result::Data>

=cut

__PACKAGE__->has_many(
    "datas", "Up::Schema::Result::Data",
    {"foreign.sheet_id" => "self.sheet_id"},
    {cascade_copy       => 0, cascade_delete => 0},
);

=head2 file

Type: belongs_to

Related object: L<Up::Schema::Result::Dataset>

=cut

__PACKAGE__->belongs_to(
    "file",
    "Up::Schema::Result::Dataset",
    {file_id       => "file_id"},
    {is_deferrable => 0, on_delete => "NO ACTION", on_update => "NO ACTION"},
);

# Created by DBIx::Class::Schema::Loader v0.07049 @ 2019-06-16 01:52:44
# DO NOT MODIFY THIS OR ANYTHING ABOVE! md5sum:rQncu8zwI1Mj9wOa4j+ohQ

# You can replace this text with custom code or comments, and it will be preserved on regeneration
1;
