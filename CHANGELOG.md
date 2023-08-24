# CHANGELOG

## v1.4 [24-Aug-2023]
### Fixed
- Fixed a bug when parsing `PARTITION BY` statements where the `STRUCT` token was in a different line 
without blank spaces.
- Commented lines (starting with double dashes --) are ignored.

## v1.3 [08-Aug-2023]
### Fixed
- Fixed a bug when parsing `JOIN` statements where the table/stream had multiple spaces
ending up in an empty table/stream name.

## v1.2 [08-Aug-2023]
### Fixed
- Fixed a bug when parsing `CREATE TABLE/STREAM WITH` statements. The term `with` was 
being added as part of the table/stream name.

## v1.1 [27-Jul-2023]
### Fixed
- Fixed a bug when parsing `PARTITION BY` statements that had one line.

## v1.0 [26-Jul-2023]
### Added
- Basic functionality: reads a ksql script and generates a nice drawing.
