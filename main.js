import * as csv from "https://deno.land/std@0.195.0/csv/mod.ts";
import { parse } from "https://deno.land/std@0.195.0/datetime/parse.ts";
import { Client } from "https://deno.land/x/postgres@v0.17.0/mod.ts";
import { format } from "https://deno.land/std@0.195.0/datetime/format.ts";

const YEAR = 112

const response = await fetch(`https://www.dgpa.gov.tw/FileConversion?filename=dgpa/files/202206/d52179b9-5e82-489b-86fd-716e959bfa5c.csv&nfix=&name=${YEAR}年中華民國政府行政機關辦公日曆表.csv`)
const big5Text = await response.arrayBuffer()
let text = new TextDecoder("big5").decode(big5Text);
const content = await csv.parse(text, { skipFirstRow: true });
let connectionString = Deno.env.get("DB") ?? 'odoo:odoo@localhost:5432/odoo'
const client = new Client(`postgres://${connectionString}`);

await client.connect();

for(const record of content.filter(r => r['是否放假'] == '0' && ['六', '日'].includes(r['星期']))) {
    let date = format(parse(record['西元日期'], 'yyyymmdd'), 'yyyy-mm-dd')
    let dayofweek = record['星期'] == '六' ? '5' : '6'
    let name = record['備註'] 
    let sql = `INSERT INTO resource_calendar_attendance
      (calendar_id, create_uid, write_uid, name, dayofweek, day_period, date_from, date_to, hour_from, hour_to, create_date, write_date)
      SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now()
      WHERE NOT EXISTS (SELECT 1 FROM resource_calendar_attendance WHERE day_period = 'morning' AND date_from = '${date}')`
    await client.queryArray(sql, [
        '1', '1', '1', name, dayofweek, 'morning', date, date, '9', '12'
    ]);
    sql = `INSERT INTO resource_calendar_attendance 
      (calendar_id, create_uid, write_uid, name, dayofweek, day_period, date_from, date_to, hour_from, hour_to, create_date, write_date)
      SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 , now(), now()
      WHERE NOT EXISTS (SELECT 1 FROM resource_calendar_attendance WHERE day_period = 'afternoon' AND date_from = '${date}')`
    await client.queryArray(sql, [
        '1', '1', '1', name, dayofweek, 'afternoon', date, date, '13', '18'
    ]);
    console.log(date, name, dayofweek)
}


for(const record of content.filter(r => r['是否放假'] == '2' && !['六', '日'].includes(r['星期']))) {
    let date = format(parse(record['西元日期'], 'yyyymmdd'), 'yyyy-mm-dd')
    let name = record['備註'] 
    let sql = `INSERT INTO resource_calendar_leaves
      (company_id, calendar_id, create_uid, write_uid, name, time_type,  date_from, date_to, create_date, write_date)
      SELECT $1, $2, $3, $4, $5, $6, $7, $8, now(), now()
      WHERE NOT EXISTS (SELECT 1 FROM resource_calendar_leaves WHERE date_from = '${date} 00:00:00')`
    await client.queryArray(sql, [
        '1', '1', '2', '2', name, 'leave', `${date} 00:00:00`, `${date} 09:00:00` 
    ]);
    console.log(date, name)
}

await client.end();