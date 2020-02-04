CREATE OR REPLACE FUNCTION cbp.f_net_weight_parser()
--DROP FUNCTION cbp.f_net_weight_parser()

returns table(_conv_weight_mt numeric, 
	      _parsed_weight text, 
	      _parsed_unit text, 
	      _parsed text, 
	      _count bigint, 
	      _weight_ratio numeric,
	      _import_id integer,
	      _source text, 
	      _bill text, 
	      _is_amendment boolean, 
	      _date date, 
	      _vessel_name text, 
	      _imo_declared integer, 
	      _bbls numeric, 
	      _weight_mt numeric, 
	      _description text, 
	      _fulltext_description tsvector, 
	      _description_id integer, 
	      _frob boolean, 
	      _bulk boolean, 
	      _container text, 
	      _port_us integer, 
	      _port_us_name text, 
	      _port_origin integer, 
	      _port_origin_name text, 
	      _shipper text, 
	      _consignee text, 
	      _record_date date)
AS
$BODY$
-- Function: cbp.f_net_weight_parser()
-- 	Parses container_description for the appropriate Net Weight. Final unit is MT.
-- New Column Descriptions:
-- 	_parsed: this is the base Net Weight string that was parsed from the container_description 
-- 	_parsed_weight: Clean numerical value (after updates) parsed from _parsed (the Net Weight string)
-- 	_parsed_unit: Clean unit parsed from _parsed (the Net Weight string)
--	_conv_weight_mt: this is the _parsed_weight converted to MT
--	_count: this count is the number of times that the Net Weight parsing regular expression matched on something in the container_description
--	_weight_ratio: parsed weight in MT over repoted weight in MT, calculated like this --> (_conv_weight_mt / _weight_mt) 

declare v_parser varchar := '((((TOTAL|NETT?\.?|TOTAL\s?NETT?)(\s{1,3}|\n)?(W\s?E\s?I\s?G\s?H\s?T?|W\s?T\.?|QUANTITY|QTY))|TOTAL\s?NETT?)\s*(\:|;|\.|\-|\,|=)?\s*(\d+[\,\.\s]?[\,\.\s]?\d*[\,\.\s]?[\,\.\s]?\d*\s?(K\s?GS?|LBS?|M\s?TS?)))';
DECLARE v_parser_total varchar := '((((TOTAL|TOTAL\s?NETT?)(\s{1,3}|\n)?(W\s?E\s?I\s?G\s?H\s?T?|W\s?T\.?|QUANTITY|QTY))|TOTAL\s?NETT?)\s*(\:|;|\.|\-|\,|=)?\s*(\d+[\,\.\s]?[\,\.\s]?\d*[\,\.\s]?[\,\.\s]?\d*\s?(K\s?GS?|LBS?|M\s?TS?)))';

BEGIN

		--Net Weight Parsing step (matches on Total Weight, if possible)
		DROP TABLE IF EXISTS t_parsing_1; 
		CREATE TEMP TABLE t_parsing_1 as
		(
		SELECT
		case 
			when a.description ~* v_parser_total THEN substring(a.description from v_parser_total)
			ELSE substring(a.description from v_parser)
		END as parsed,
		* from cbp.imports_combined a
		);
		
		drop table if exists t_parsing_2; 
		create temp table t_parsing_2 as
		(
		select 
		substring(parsed from '\d.*\d') as parsed_weight,
		substring(parsed from 'K\s?G\s?S?|M\s?TS?|L\s?B\s?S?') as parsed_unit,
		* from t_parsing_1
		);
		
		--select * from t_parsing_2 where parsed IS NOT NULL order by parsed_unit

		--Data Wrangling step
		drop table if exists t_parsing_3; 
		create temp table t_parsing_3 as 
		(
		select * from t_parsing_2 where parsed iS NOT NULL
		);

	--select * from t_parsing_3 where parsed_weight ~* '[\,]' order by parsed_weight

			--Removes Spaces
			update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\s', '', 'g') where parsed_weight ~* '\s';
			update t_parsing_3 set parsed_unit = regexp_replace(parsed_unit, '\s', '', 'g') where parsed_unit ~* '\s';
			--Fits weight to NUMERIC format
			update t_parsing_3 set parsed_weight = replace(parsed_weight, '.', '') where parsed_weight ~* '^(?:\d{0,3}\.)?\d{3}\,\d{2,3}$' and parsed_unit !~* 'MTS?';
			update t_parsing_3 set parsed_weight = replace(parsed_weight, ',', '') where parsed_weight ~* '^\d{1,3}\,\d{3}(?:\.\d{2,3})?$' and parsed_unit !~* 'MTS?';
			update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\.', '') where parsed_weight ~* '^\d{1,3}\.\d{3}\.(?:\d{2,3})?$' and parsed_unit !~* 'MTS?';
			update t_parsing_3 set parsed_weight = replace(parsed_weight, '.', '') where parsed_weight ~* '^\d{2}\.\d{3}$' and parsed_unit !~* 'MTS?';
			update t_parsing_3 set parsed_weight = replace(parsed_weight, '.', '') where parsed_weight ~* '^\d\.\d{3}$' and parsed_unit !~* 'MTS?';
			update t_parsing_3 set parsed_weight = replace(parsed_weight, ',', '.') where parsed_weight ~* '^\d{5,6}\,\d{2,3}$' and parsed_unit !~* 'MTS?';
			update t_parsing_3 set parsed_weight = Replace(parsed_weight, ',', '.') where parsed_weight ~* '^\d{2,3}\,\d{2}$' and parsed_unit !~* 'MTS?';
			
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\.$', '') where parsed_weight ~* '^\d{5,6}\.\d{2,3}\.$' and parsed_unit !~* 'MTS?'; --From here these are new fits for cbp.imports_combined
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\,', '.', 'g') where parsed_weight ~* '^\d{4}[\,]\d{2,3}$' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\,', '', 'g') where parsed_weight ~* '^\d{4}[\,]\d{3}[\.]\d{2}$' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\,', '.', 'g') where parsed_weight ~* '^\d{1,2}[\,]\d{1,2}$|^\d{3,4}[\,]\d{1,2}$' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\,', '') where parsed_weight ~* '^\d{1,3}[\,]\d{3}([\,\.]\d)?' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '\.', '') where parsed_weight ~* '^\d{1,3}[\.]\d{3}([\,\.]\d)?' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = replace(parsed_weight, ',', '.') where parsed_weight ~* '^\d{3,7}[\,]\d' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '[\.][\,]', '.') where parsed_weight ~* '[\.][\,]' and parsed_unit !~* 'MTS?';
			--update t_parsing_3 set parsed_weight = regexp_replace(parsed_weight, '[\,][\.]', '') where parsed_weight ~* '[\,][\.]' and parsed_unit !~* 'MTS?';
			
		--select * from t_parsing_3 order by parsed_unit

		--Takes all NOFIT DATA (data that can not currently be cast to numeric and moves it into the temp table abyss
		drop table if exists t_nofit_0;
		create temp table t_nofit_0 as
		(
		select * from t_parsing_3 where parsed_weight ~* '[\,\\\/'']' OR parsed_weight ~* '[\.].*[\.]'
		);

		update t_parsing_3 set parsed_weight = null where import_id in (select import_id from t_nofit_0);

		--MT Conversion step
		ALTER TABLE t_parsing_3 ADD conv_weight_mt NUMERIC;

		update t_parsing_3 set conv_weight_mt = round((parsed_weight::NUMERIC/1000), 3) where parsed_unit ~* 'KGS?';
		update t_parsing_3 set conv_weight_mt = round((parsed_weight::NUMERIC*0.000453592), 3) where parsed_unit ~* 'LBS?';
		update t_parsing_3 set conv_weight_mt = round(parsed_weight::numeric, 3) where parsed_unit ~* 'MTS?';

		--select * from t_parsing_3 order by conv_weight_mt desc

		--Condition Check step
			--Match Counter (Makes conv_weight_mt NULL if container has multiple ambiguous Net Weights in its description)
			drop table if exists t_match_count_0; 
			create temp table t_match_count_0 as
			(
			select 
			unnest(regexp_matches(description, v_parser, 'g')) as parsed,
			* from cbp.imports_combined
			WHERE description ~* v_parser
			and description !~* v_parser_total
			);
			
			drop table if exists t_match_count_1;
			create temp table t_match_count_1 as
			(
			select count(*), import_id from t_match_count_0 group by import_id
			);
			
			drop table if exists t_match_count_2; 
			create temp table t_match_count_2 as
			(
			select a.conv_weight_mt, a.parsed_weight, a.parsed_unit, a.parsed, b.count, a.import_id, a.source, a.bill, a.is_amendment, a.date, a.vessel_name, a.imo_declared, a.bbls, a.weight_mt, a.description, a.fulltext_description, a.description_id, a.frob, a.bulk, a.container, a.port_us, a.port_us_name, a.port_origin, a.port_origin_name, a.shipper, a.consignee, a.record_date
			from t_parsing_3 a
			left join t_match_count_1 b on (a.import_id = b.import_id)
			);
					
			update t_match_count_2 set conv_weight_mt = null where count > 1;

			--Ratio Check: adds a new column: weight_ratio = (conv_weight_mt / weight_mt)
			alter table t_match_count_2 add weight_ratio numeric;
			update t_match_count_2 set weight_ratio = round((conv_weight_mt / weight_mt), 3) where weight_mt != 0;
			
		
		--Final Table Creation
		drop table if exists t_final;
		create temp table t_final as
		(
		select conv_weight_mt, parsed_weight, parsed_unit, parsed, count, weight_ratio, import_id, source, bill, is_amendment, date, vessel_name, imo_declared, bbls, weight_mt, description, fulltext_description, description_id, frob, bulk, container, port_us, port_us_name, port_origin, port_origin_name, shipper, consignee, record_date
		from t_match_count_2
		where conv_weight_mt is not null
		order by weight_ratio desc
		);

		return query	
		select * from t_final order by count;
		
END;
$BODY$
	LANGUAGE plpgsql volatile
	cost 100
	rows 1000;
alter function cbp.f_net_weight_parser()
	owner to adam_turner
;

select * from cbp.f_net_weight_parser()
