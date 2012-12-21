
CREATE TEMPORARY FUNCTION multisplode AS 'com.sonamine.hive.udf.GenericUDTFMultisplode';
CREATE TEMPORARY FUNCTION get_cut AS 'com.sonamine.hive.udf.GenericUDFCut';
CREATE TEMPORARY FUNCTION to_array AS 'com.sonamine.hive.udf.UDAFToArray';
CREATE TEMPORARY FUNCTION to_map AS 'com.sonamine.hive.udf.UDAFToMap';
CREATE TEMPORARY FUNCTION to_ordered_map AS 'com.sonamine.hive.udf.UDAFToOrderedMap';
CREATE TEMPORARY FUNCTION hive_melt AS 'com.sonamine.hive.udf.GenericUDTFMelt';
CREATE TEMPORARY FUNCTION rbinom AS 'com.sonamine.hive.udf.GenericUDFRbinom';
CREATE TEMPORARY FUNCTION array_max AS 'com.sonamine.hive.udf.GenericUDFArrayMax';
CREATE TEMPORARY FUNCTION array_min AS 'com.sonamine.hive.udf.GenericUDFArrayMin';
