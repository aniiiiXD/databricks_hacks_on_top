-- Databricks DLT SQL
-- Gold Circular Chunks: RBI circulars chunked for vector search / RAG
-- Uses the AGGREGATE() accumulator pattern for intelligent text splitting
-- Runs inside a Lakeflow Declarative Pipeline

CREATE OR REFRESH MATERIALIZED VIEW gold_circular_chunks
COMMENT 'RBI circulars split into ~4000 character chunks at paragraph boundaries for vector search and RAG retrieval. Each chunk retains source metadata for citation.'
AS
WITH chunked AS (
    SELECT
        circular_id,
        title,
        circular_date,
        department,
        topic_label,
        url,
        -- AGGREGATE accumulator: splits text at \n\n boundaries, targeting ~4000 char chunks
        -- This is the same pattern used in the bharatbricksiitb starter kit
        AGGREGATE(
            SPLIT(full_text, '\n\n'),
            -- Accumulator: array of completed chunks + current in-progress chunk
            named_struct(
                'chunks', ARRAY(CAST('' AS STRING)),
                'cur', CAST('' AS STRING)
            ),
            -- Iterate: if adding next paragraph exceeds 4000 chars, flush current chunk
            (acc, paragraph) ->
                CASE
                    WHEN LENGTH(acc.cur) + LENGTH(paragraph) + 2 > 4000
                    THEN named_struct(
                        'chunks', CONCAT(acc.chunks, ARRAY(acc.cur)),
                        'cur', paragraph
                    )
                    ELSE named_struct(
                        'chunks', acc.chunks,
                        'cur', CASE
                            WHEN LENGTH(acc.cur) = 0 THEN paragraph
                            ELSE CONCAT(acc.cur, '\n\n', paragraph)
                        END
                    )
                END,
            -- Finalize: flush the last in-progress chunk
            acc -> CONCAT(acc.chunks, ARRAY(acc.cur))
        ) AS chunks_array
    FROM silver_circulars
    WHERE full_text IS NOT NULL AND LENGTH(TRIM(full_text)) > 0
)
SELECT
    -- Composite chunk ID for unique identification
    CONCAT(circular_id, '_chunk_', CAST(chunk_index AS STRING)) AS chunk_id,
    circular_id,
    title,
    circular_date,
    department,
    topic_label,
    url,
    chunk AS chunk_text,
    chunk_index,
    LENGTH(chunk) AS chunk_length,
    -- Total chunks for this circular (useful for RAG context)
    SIZE(chunks_array) AS total_chunks_in_circular,
    -- Citation string for RAG responses
    CONCAT('RBI Circular: ', title, ' (', COALESCE(CAST(circular_date AS STRING), 'undated'), ') - ', department) AS citation
FROM chunked
LATERAL VIEW POSEXPLODE(chunks_array) AS chunk_index, chunk
WHERE LENGTH(TRIM(chunk)) > 0;
