
CREATE TEMPORARY FUNCTION multisplode AS 'com.jfolson.hive.udf.GenericUDTFMultisplode';
CREATE TEMPORARY FUNCTION get_cut AS 'com.jfolson.hive.udf.GenericUDFCut';
CREATE TEMPORARY FUNCTION to_array AS 'com.jfolson.hive.udf.UDAFToArray';
CREATE TEMPORARY FUNCTION to_map AS 'com.jfolson.hive.udf.UDAFToMap';
CREATE TEMPORARY FUNCTION to_ordered_map AS 'com.jfolson.hive.udf.UDAFToOrderedMap';
CREATE TEMPORARY FUNCTION hive_melt AS 'com.jfolson.hive.udf.GenericUDTFMelt';
CREATE TEMPORARY FUNCTION rbinom AS 'com.jfolson.hive.udf.GenericUDFRbinom';
CREATE TEMPORARY FUNCTION array_max AS 'com.jfolson.hive.udf.GenericUDFArrayMax';
CREATE TEMPORARY FUNCTION array_min AS 'com.jfolson.hive.udf.GenericUDFArrayMin';
