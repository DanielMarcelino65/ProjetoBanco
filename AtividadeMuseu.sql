CREATE TABLE registros (
	gbifID BIGINT PRIMARY KEY,
	datasetKey UUID,
	occurrenceID TEXT,
	license VARCHAR(255),
	rightsHolder TEXT,
	typeStatus VARCHAR(255),
	establishmentMeans VARCHAR(255),
	lastInterpreted TIMESTAMP,
	mediaType TEXT
);

CREATE TABLE recorded_by (
    recordedBy_id SERIAL PRIMARY KEY,
    recordedByPerson TEXT
);

CREATE TABLE registros_recorded_by (
    gbifID BIGINT,
    recordedBy_id INT,
    FOREIGN KEY (gbifID) REFERENCES registros(gbifID),
    FOREIGN KEY (recordedBy_id) REFERENCES recorded_by(recordedBy_id)
);

CREATE TABLE issues (
    issue_id SERIAL PRIMARY KEY,
    issue TEXT UNIQUE
);

CREATE TABLE registros_issues (
    gbifID BIGINT,
    issue_id INT,
    FOREIGN KEY (gbifID) REFERENCES registros(gbifID),
    FOREIGN KEY (issue_id) REFERENCES issues(issue_id)
);


CREATE TABLE taxonomica (
	gbifID BIGINT,
	kingdom VARCHAR(255),
	phylum VARCHAR(255),
	class VARCHAR(255),
	"order" VARCHAR(255),
	family VARCHAR(255),
	genus VARCHAR(255),
	species VARCHAR(255),
	infraspecificEpithet TEXT,
	taxonRank VARCHAR(255),
	scientificName TEXT,
	verbatimScientificName TEXT,
	verbatimScientificNameAuthorship TEXT,
	FOREIGN KEY (gbifID) REFERENCES registros(gbifID)
);

CREATE TABLE localizacao (
	gbifID BIGINT,
	countryCode VARCHAR(255),
	locality TEXT,
	stateProvince TEXT,
	decimalLatitude NUMERIC,
	decimalLongitude NUMERIC,
	coordinateUncertaintyInMeters INTEGER,
	coordinatePrecision NUMERIC,
	FOREIGN KEY (gbifID) REFERENCES registros(gbifID)
);

CREATE TABLE eventos (
	gbifID BIGINT,
	eventDate DATE,
	day INTEGER,
	month INTEGER,
	year INTEGER,
	FOREIGN KEY (gbifID) REFERENCES registros(gbifID)
);

CREATE TABLE metricas (
	gbifID BIGINT,
	individualCount INTEGER,
	elevation NUMERIC,
	elevationAccuracy NUMERIC,
	depth NUMERIC,
	depthAccuracy NUMERIC,
	FOREIGN KEY (gbifID) REFERENCES registros(gbifID)
);

CREATE TABLE catalogo (
	gbifID BIGINT,
	taxonKey BIGINT,
	speciesKey BIGINT,
	basisOfRecord VARCHAR(255),
	institutionCode VARCHAR(255),
	collectionCode VARCHAR(255),
	catalogNumber TEXT,
	recordNumber TEXT,
	identifiedBy TEXT,
	dateIdentified DATE,
	FOREIGN KEY (gbifID) REFERENCES registros(gbifID)
);

CREATE TABLE publicacao (
	gbifID BIGINT,
	publishingOrgKey UUID,
	FOREIGN KEY (gbifID) REFERENCES registros(gbifID)
);

CREATE TABLE log_eventos (
	log_id SERIAL PRIMARY KEY,
	tipo_evento VARCHAR(50),
	tabela_afetada VARCHAR(255),
	registro_afetado BIGINT,
	detalhes TEXT,
	timestamp_evento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE metricas

CREATE OR REPLACE FUNCTION log_insercao_registros() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO log_eventos(tipo_evento, tabela_afetada, registro_afetado, detalhes)
	VALUES ('INSERT', 'registros', NEW.gbifID, 'Registro Inserido');
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_insercao_registros
AFTER INSERT ON registros
FOR EACH ROW
EXECUTE FUNCTION log_insercao_registros();

CREATE OR REPLACE FUNCTION log_delecao_registros() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO log_eventos(tipo_evento, tabela_afetada, registro_afetado, detalhes)
	VALUES ('DELETE', 'registros', OLD.gbifID, 'Registro deletado');
	RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_delecao_registros
BEFORE DELETE ON registros
FOR EACH ROW
EXECUTE FUNCTION log_delecao_registros();

CREATE OR REPLACE FUNCTION log_atualizacao_registros() RETURNS TRIGGER AS $$
BEGIN
	INSERT INTO log_eventos(tipo_evento, tabela_afetada, registro_afetado, detalhes)
	VALUES ('UPDATE', 'registros', NEW.gbifID, 'Registro atualizado');
	RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_atualizacao_registros
AFTER UPDATE ON registros
FOR EACH ROW
EXECUTE FUNCTION log_atualizacao_registros();


CREATE OR REPLACE FUNCTION drop_null_columns()
RETURNS void AS $$
DECLARE
    r RECORD;
    query TEXT;
BEGIN
    FOR r IN SELECT table_schema, table_name, column_name
             FROM information_schema.columns
             WHERE table_schema = 'public'
    LOOP
        EXECUTE format('SELECT count(*) FROM %I.%I WHERE %I IS NOT NULL', 
                        r.table_schema, r.table_name, r.column_name) INTO query;

        IF query = '0' THEN
            EXECUTE format('ALTER TABLE %I.%I DROP COLUMN %I', 
                            r.table_schema, r.table_name, r.column_name);
            RAISE NOTICE 'Coluna % removida da tabela %', r.column_name, r.table_name;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT drop_null_columns();

CREATE VIEW distribuicao_geografica AS
SELECT countryCode, COUNT(*) AS total_registros
FROM localizacao
GROUP BY countryCode;

select * from distribuicao_geografica

CREATE VIEW analise_anual_registros AS
SELECT year, COUNT(*) AS total_registros
FROM eventos
GROUP BY year;

select * from analise_anual_registros

CREATE VIEW registros_por_categoria_taxonomica AS
SELECT kingdom, phylum, class, COUNT(*) AS total_registros
FROM taxonomica
GROUP BY kingdom, phylum, class;

select * from registros_por_categoria_taxonomica

\copy registros FROM 'C:\Users\danma\Downloads\registros_teste.csv' WITH DELIMITER ',' CSV HEADER;


select * from analise_anual_registros

	
CREATE VIEW top_contributors AS
SELECT 
    rb.recordedByPerson,
    COUNT(rrb.gbifID) AS num_registros
FROM 
    recorded_by rb
JOIN 
    registros_recorded_by rrb ON rb.recordedBy_id = rrb.recordedBy_id
GROUP BY 
    rb.recordedByPerson
ORDER BY 
    num_registros DESC
LIMIT 10;

select * from top_contributors

CREATE VIEW view_erros_registros AS
SELECT 
    i.issue,
    COUNT(ri.gbifID) AS quantidade_registros
FROM 
    issues i
JOIN 
    registros_issues ri ON i.issue_id = ri.issue_id
GROUP BY 
    i.issue;

select * from view_erros_registros


CREATE VIEW distribuicao_taxonomica AS
SELECT 
    t.kingdom,
    t.phylum,
    t.class,
    t.order,
    t.family,
    t.genus,
    t.species,
    COUNT(*) AS num_registros
FROM 
    taxonomica t
GROUP BY 
    ROLLUP(t.kingdom, t.phylum, t.class, t.order, t.family, t.genus, t.species);

select * from distribuicao_taxonomica



select * from log_eventos
select * from registros
select * from recorded_by
select * from registros_recorded_by
select * from issues
select * from registros_issues
select * from taxonomica
select * from localizacao
select * from eventos
select * from catalogo
select * from publicacao




