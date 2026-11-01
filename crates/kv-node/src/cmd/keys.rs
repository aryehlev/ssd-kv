//! Key commands (DESIGN §8.4, §16.1, §16.2): DEL / UNLINK (reply after the durable delete),
//! EXISTS (duplicates counted each time), TYPE, RENAME / RENAMENX (TTL carried, crash-atomic,
//! §14.6), COPY (M1.3) and RANDOMKEY (a random non-empty table of a random partition, one
//! partition per call).
