Q: Why is it when deciding Full Refresh vs Incremental saying "Incremental obviously" is wrong?

A: Because every pipeline starts as a full refresh and that full refresh is correct until it is not.

---

Q: Full Refresh vs Incremental should be framed as a cost function. What does that mean?

A: Full refresh cost grows linearly with source table size. Incremental has fixed overhead - change detection, merge logic, state tracking - plus variable cost that grows with change volume, not total data. At some point the lines cross. 

---

Q: Full Refresh vs Incremental Tradeoffs

A: Simple: SELECT * and replace | Complex: detect changes, merge, track state
   Always correct - no state to corrupt | Can accumulate drift if change detection fails
   Cost grows with total data size | Cost grows with change volume, not total size
   10M rows: 3 min, 100M: 30 min, 1B: 5+ hrs | 10K changes on 1B rows: 20 seconds

---

Q: Where does the typical crossover for Full Refresh vs Incremental land, and how can you go deeper like the interviewer will expect you to? 

A: The crossover typically lands between 10-50 million rows for most warehouse workloads. 
   
   Compute-time billing pushes the crossover earlier because full refreshes burn credit     
   linearly. This is like Snowflake or Redshift Serverless where you pay for uptime and 
   compute. 

   Storage-based pricing pushes it later because incremental state has its own cost. This is 
   like object storage, or iceberg/delta tables. Incremental pipelines store state tables, 
   snapshots, change logs, etc., so incremental must save enough compute to justify extra 
   storage cost 

   Red flag: candidates who give a number without explaining what drives it.

---

Q: When you mention incremental loading always mention dbt as it's the most common answer. What does the config block look like?

A: 	{{
      config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
      )
    }}

    select ...

    from ...

    {% if is_incremental() %}
      where created_at > (select max(created_at) from {{this}} )
    {% endif %}

   On first run, dbt builds the full table. On subsequent runs, it only processes rows newer  
   than the current maximum timestamp. The unique_key ensures upsert behavior - same event_id 
   arriving twice updates rather than duplicates. 

---

Q: When you write an incremental dbt model a common follow-up question is "What happens if    
   your timestamp has late-arriving data?"

A: Overlap windows or a lookback buffer 

---

Q: In incremental pipelines, how do you track where you left off?

A: Through a watermark. A watermark is a checkpoint that records how far you have processed. 
   The simplest version is MAX(updated_at) stored in a control table. Each run reads the 
   watermark, processes everything newer, then advances it. The trap: this only works for 
   append-only data. If a row's updated_at was set to yesterday but the row was modified 
   today, a timestamp watermark misses it. Say this before the interviewer asks

---

Q: When an interviewer asks "full refresh or incremental", what you should do instead of just 
   picking one? 

A: Start with the crossover: 'At our current volume of X rows with Y% daily change rate, 
   incremental saves Z compute hours per run'

   Name the strategy: merge, append, or delete+insert - and why

   Close with the safety net: 'We run a weekly full refresh as a consistency check to catch 
   any drift'

   Senior signal: mention the dbt incremental config and the specific strategy (merge vs   
   delete+insert)

--- 

Q: What is the simplest CDC approach and what are it's drawbacks? 

A: Using each source table's updated_at column, the pipeline queries WHERE updated_at > 
   last_watermark.

   This only works if every modification to every row reliably updates the   
   timestamp. The moment one code path or one bulk operation skips the timestamp update, your 
   pipeline silently drops changes. 

   This method also can't detect deletes. When a row is removed it dissapears while the 
   incremental_strategy copy is still in the warehouse. You handle this through periodic full-
   refresh reconciliation, soft deletes in the source, or switch to log-based CDC. Mention
   these drawback before the interviewer does. 

---

Q: What is the log-based CDC?

A: When you reads the database's change log directly. Every INSERT, UPDATE, and DELETE is 
   captured because the log is the source of truth the database itself uses for replication 
   and crash recovery. Debezium is the most common open-source tool - it reads the PostgreSQL 
   WAL (or MySQL binlog) and emits change events to Kafka. 

   In interviews, this is expected knowledge. Knowing that managed tools like Fivetran and 
   DMS abstract this is also sufficient.

---

Q: What are managed CDC tools and when should I use them over custom tools? 

A: Managed CDC tools are services like Fivetran and Airbyte that abstract the WAL complexity. 
   They handle configuration, schema mapping, and delivery guarantees. 

   If you have fewer than 50 tables and no sub-minute latency requirement, use Fivetran. If 
   you need real-time CDC with custom routing logic, build on Debezium + Kafka. 

--- 

Q: Once you have a stream of changes you apply them through a MERGE statement. What does that 
   look like?

A: MERGE INTO warehouse.orders AS target
   USING staging.orders_delta AS source
   ON target.order_id = source.order_id
   WHEN MATCHED THEN UPDATE SET target.status = source.status, target.updated_at = source.updated_at, target.amount = source.amount
   WHEN NOT MATCHED THEN INSERT (order_id, status, updated_at, amount) 
   VALUES (source.order_id, source.status, source.updated_at, source.amount)

--- 

Q: What is the impact and typical handling of a schema change where a column is added? 

A: Low impact and it's typically handled by appending the column to the target table and 
   backfilling nulls

---

Q: What is the impact and typical handling of a schema change where a column is renamed

A: Medium impact and it's typically handled by an alias in the transformation layer

---

Q: What is the impact and typical handling of a schema change where a columns data type is 
   changed?

A: High Impact and it's typically handled by a migration script plus a backfill

--- 

Q: What is the impact and typical handling of a schema change where a column is dropped?

A: High impact and it's typically handled by a deprecation period before removal

--- 

Q: What is the impact and typical handling of a schema change where a primary key is changed?

A: Critical impact and it's typically handled by a full rebuild

--- 

Q: What is a schema registry? 

A: A schema registry is a versioned catalog of every schema your pipeline has seen. When a 
   source schema changes, the registry detects it, classifies the change as breaking or non-
   breaking, and either auto-applies it or blocks the pipeline and alerts the team. Schema 
   registries like Confluent Schema Registry (for Kafka/Avro) are the tools to name-drop. 

--- 

Q: What does backwards compatibility refer to when it comes to schema registries and how do 
   you handle breaking changes? 

A: Backwards compatibility means new data can be read by old consumers. A new nullable column 
   is backwards compatible - old consumers simply ignore it. A removed column is not - old  
   consumers that expect it will fail. The follow-up question: 

   "How do you handle a breaking change?" Your answer: deprecation period, consumer 
   notification, and a migration window. Never a surprise ALTER TABLE.

--- 

Q: What does it mean to say the hardest part of schema evolution is not technical but 
   organizational?

A: When you add a column to an upstream table, who needs to know? Every pipeline, dashboard, 
   and ML model that reads from any downstream table that might propagate the change. Schema 
   registries automate detection, but notification requires a human process: a changelog, a 
   Slack channel, a review step in CI. Say this and you are demonstrating real production 
   experience.

--- 

Q: What are the 4 dbt on_schema_change options for incremental models? 

A: ignore is the default. If a new column is added to the model, dbt ignores it and does
   not add it to the existing target table. If a column is removed from the model, the run
   fails.

   fail stops the run whenever the model and target table schemas differ. Use this when all
   schema changes must be reviewed and handled manually.

   append_new_columns adds newly introduced columns to the existing target table. For
   example, if order_status is added to the model, dbt alters the target table to include
   order_status. It does not remove columns that are no longer selected by the model 
  
   sync_all_columns makes the target table match the model. It adds newly introduced
   columns, removes columns that are no longer present, and handles supported data type
   changes.
   
   None of these options populate historical rows when a new column is added. Existing rows
   will have nulls until you perform a manual backfill or run dbt run --full-refresh.

--- 

Q: How do you backfill

A: Instead of reprocessing the entire table (expensive, risky) or individual rows (complex, 
   slow), you reprocess entire date partitions. This gives you a natural unit of work that is 
   independently verifiable and restartable. I delete the target partition, re-extract from 
   source, and re-insert. Running it twice produces the same result making backfills 
   idempotent.

---

Q: What does it mean for an operation to be idempotent and what does it mean in the context 
   of backfills 

A: An idempotent operation produces the same result regardless of how many times it runs. For 
   backfills, this means: if your pipeline crashes halfway through reprocessing January and 
   you restart it, you get exactly the correct January data - not duplicates, not partial 
   results. The delete-then-insert pattern achieves this naturally.

--- 

Q: Other then DELETE partition + INSERT backfill designs what other backfill design can be 
   idempotent and what would stop it from being so

A: MERGE-based backfills ON unique_key also known as upserts are also idempotent as long as 
   your unique key is stable. 

---

Q: What does the backfill SQL look like and what is the Red Flag to avoid when using INSERT 
   INTO

A: DELETE
   FROM warehouse.events
   WHERE event_date BETWEEN '2026-03-01' AND '2026-03-31' ; INSERT INTO warehouse.events

   SELECT  *
   FROM transform_events(source_ref = > 'raw.events',  start_date = > '2026-03-01',  end_date = > '2026-03-31') ;

   Red flag: using INSERT INTO without a preceding DELETE - that appends duplicates on every 
   retry.

--- 

Q: 






