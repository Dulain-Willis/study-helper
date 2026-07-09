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

Q: What is the simplest CDC approach and what are it's drawback? 

A: Using each source table's updated_at column, the pipeline queries WHERE updated_at > 
   last_watermark.

   This only if every modification to every row reliably updates the   
   timestamp. The moment one code path or one bulk operation skips the timestamp update, your 
   pipeline silently drops changes. 

   This method also can't detect deletes. When a row is removed it dissapearz while the 
   incremental_strategy copy is still in the warehouse. You handle this through periodic full-
   refresh reconciliation, soft deletes in the source, or switch to log-based CDC. Mention
   these drawback before the interviewer does. 

---

Q: What is the log-based CDC?

A: When you reads the database's chnage log directly. Every INSERT, UPDATE, and DELETE is 
   captured because the log is the source of truth the database itself uses for replication 
   and crash recovery. Debezium is the most common open-source tool - it reads the PostgreSQL 
   WAL (or MySQL binlog) and emits change events to Kafka. 

   In interviews, this is expected knowledge. Knowing that managed tools like Fivetran and 
   DMS abstract this is also sufficient.

---

Q: 
