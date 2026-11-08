// Real Codex app-server -> Rust normalizer -> real importer/Pi SQLite. All native
// history and mutations belong to this disposable test's isolated CODEX_HOME.
import assert from 'node:assert/strict';
import {appendFile,mkdir,readFile,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {createInterface} from 'node:readline';
import {pathToFileURL} from 'node:url';
import {DatabaseSync} from 'node:sqlite';
import {stat} from 'node:fs/promises';
import {BACKGROUND_CONTEXT} from '@earendil-works/chord/context';
import {openNodeSqliteDatabase} from '@earendil-works/chord/sqlite/node';
import {createModels} from '@earendil-works/pi-ai';
import {createRegistry,Harness} from '@earendil-works/pi-durable';
import {SqliteStorage} from '@earendil-works/pi-durable/storage/sqlite';
const runtime=pathToFileURL(process.argv[1]),home=process.argv[2],id=process.argv[3];
const {CodexImporter}=await import(new URL('./importer.js',runtime));
const {HistoryStore,ImportedItemEntry}=await import(new URL('./history.js',runtime));
const input=createInterface({input:process.stdin}),pending=[];
input.on('line',line=>pending.shift()(JSON.parse(line)));
const request=value=>new Promise(resolve=>{pending.push(resolve);process.stdout.write(JSON.stringify(value)+'\n')});
const host={host_id:'synthetic-host',name:'synthetic-host',rex_server:null,agent_program:'beep-boop',home_directory:'/example',coordinator:null,
 launch:{state_dir:null,runtime:null,config:null,instructions:null,model:null}};
const folder=join(home,'sessions/2026/01/02'),path=join(folder,`rollout-2026-01-02T12-00-00-${id}.jsonl`);
await mkdir(folder,{recursive:true});
await writeFile(join(home,'config.toml'),'model_provider = "openai"\n');
const line=(type,payload)=>JSON.stringify({timestamp:'2026-01-02T12:00:00Z',type,payload})+'\n';
const message=(index,text)=>line('event_msg',{type:'item_completed',thread_id:id,turn_id:'turn-a',
 item:{type:'AgentMessage',id:`item-${index}`,content:[{type:'Text',text}],phase:'final_answer'},completed_at_ms:1767355200000});
await writeFile(path,line('session_meta',{id,session_id:id,timestamp:'2026-01-02T12:00:00Z',cwd:'/',
 originator:'synthetic-incremental-test',cli_version:'0.0.0',source:'cli',model_provider:'openai',history_mode:'legacy'})+
 line('event_msg',{type:'user_message',message:'Read this invented history.',images:[],local_images:[],text_elements:[]})+
 line('event_msg',{type:'task_started',turn_id:'turn-a',root_turn_id:'turn-a',started_at:1767355200,model_context_window:null})+
 Array.from({length:80},(_,index)=>message(index,`Original ${index}`)).join(''));
// Legacy discovery establishes real native metadata. Promote only this fixture
// to the same persisted paginated mode used by current Codex conversations.
await request({type:'list',archived:false,cursor:null});
await writeFile(path,(await readFile(path,'utf8')).replace('"history_mode":"legacy"','"history_mode":"paginated"'));
const native=new DatabaseSync(join(home,'state_5.sqlite'));
native.prepare('UPDATE threads SET history_mode=? WHERE id=?').run('paginated',id);native.close();
await request({type:'read',thread_id:id,cursor:null});
let ordinal=3;
async function project(updates){
 const db=new DatabaseSync(join(home,'thread_history_1.sqlite'));
 try{
  db.exec('BEGIN');
  db.prepare('INSERT OR IGNORE INTO thread_turns(thread_id,turn_id,rollout_ordinal,status) VALUES(?,?,?,?)').run(id,'turn-a',2,'inProgress');
  const upsert=db.prepare(`INSERT INTO thread_items(thread_id,turn_id,item_id,rollout_ordinal,updated_at_ordinal,created_at_ms,completed_at_ms,item_type,item_json)
   VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(thread_id,turn_id,item_id) DO UPDATE SET updated_at_ordinal=excluded.updated_at_ordinal,item_json=excluded.item_json`);
  for(const [index,text] of updates){
   upsert.run(id,'turn-a',`item-${index}`,ordinal,ordinal,1767355200000,1767355200000,'agentMessage',JSON.stringify({type:'agentMessage',id:`item-${index}`,text,phase:'final_answer'}));ordinal++;
  }
  db.prepare(`INSERT INTO thread_history_projection_state(thread_id,next_rollout_byte_offset,next_rollout_ordinal) VALUES(?,?,?)
   ON CONFLICT(thread_id) DO UPDATE SET next_rollout_byte_offset=excluded.next_rollout_byte_offset,next_rollout_ordinal=excluded.next_rollout_ordinal`).run(id,(await stat(path)).size,ordinal);
  db.exec('COMMIT');
 }finally{db.close()}
}
await project(Array.from({length:80},(_,index)=>[index,`Original ${index}`]));
async function append(updates){await appendFile(path,updates.map(([index,text])=>message(index,text)).join(''));await project(updates)}
async function open(){
 const session=await Harness.open(await SqliteStorage.open(await openNodeSqliteDatabase(join(home,'consumer.sqlite'))),
  {models:createModels(),registry:createRegistry(),identity:{attribution:null}},BACKGROUND_CONTEXT);
 const store=new HistoryStore(session,join(home,'assets'));
 return {session,store,importer:new CodexImporter(session,store,join(home,'assets'))};
}
let app=await open(),transferred=0,readCalls=0,failAt=Infinity,oldRevision=false;
async function run(){
 transferred=0;readCalls=0;
 return app.importer.run([host],[host.host_id],[],async command=>{
  assert.equal(command.type,'on_node');assert.equal(command.command.type,'history');
  const operation=command.command.request;
  if(operation.type==='read'){
   if(++readCalls===failAt)throw Error('Synthetic transport unavailable after a committed page');
   if(oldRevision&&operation.cursor!==null){
    const cursor=JSON.parse(operation.cursor);cursor.projection.revision=0;
    operation.cursor=JSON.stringify(cursor);oldRevision=false;
   }
  }
  const response=await request(operation);
  if(response.type==='read')transferred+=response.items.length;
  return {type:'history',response};
 },()=>{},async()=>{});
}
async function entries(){
 const mapping=await app.store.get(id);assert.ok(mapping);
 const conversation=await app.store.conversation(mapping.conversation_id);assert.ok(conversation);
 let cursor,items=[];
 do{const page=await conversation.entries({order:'ascending'},100,cursor,BACKGROUND_CONTEXT);items.push(...page.items.filter(ImportedItemEntry.is));cursor=page.next}while(cursor!==undefined);
 assert.deepEqual((await conversation.context(BACKGROUND_CONTEXT)).messages,[]);
 return items;
}
async function reopen(){await app.session.close(BACKGROUND_CONTEXT);app=await open()}
try{
 const first=await run();assert.equal(first.phase,'complete');
 assert.ok(first.items_added>=80,JSON.stringify(first));
 const before=await entries();assert.equal(before.length,first.items_added);
 await reopen();
 await append([[80,'Appended answer'],[0,'Updated earlier answer']]);
 const second=await run();assert.equal(second.phase,'complete');
 assert.equal(transferred,2,'Appending a message and updating an older item must transfer only those two native items after a completed import');
 assert.equal(second.items_added,2);assert.equal(second.items_unchanged,0);
 const after=await entries();assert.equal(after.length,before.length+2);
 for(const original of before)assert.deepEqual(after.find(entry=>entry.id===original.id),original);
 assert.equal(after.find(entry=>entry.data.item.text==='Updated earlier answer').data.item.order,before.find(entry=>entry.data.item.id==='item-0').data.item.order);
 assert.equal(after.find(entry=>entry.data.item.text==='Appended answer').data.item.order,before.length);
 await reopen();
 await append(Array.from({length:55},(_,index)=>[81+index,`Later ${index}`]));
 failAt=2;assert.equal((await run()).phase,'partial');assert.equal(transferred,25);
 const partial=await app.store.checkpoint(id,host.host_id);assert.ok(partial.next_cursor);assert.ok(partial.resume_cursor);
 await reopen();await append([[136,'Appended during interrupted catch-up']]);
 failAt=Infinity;assert.equal((await run()).phase,'complete');
 assert.equal(transferred,31,'Resume the committed change position even when another message changes the source version');
 await reopen();assert.equal((await run()).phase,'complete');assert.equal(transferred,0);
 oldRevision=true;
 // Force a read without changing native evidence; an older reader checkpoint
 // must invalidate once and preserve every already-retained item identity.
 const saved=await app.store.checkpoint(id,host.host_id),mapping=await app.store.get(id);
 await app.store.import({host_id:host.host_id,thread:mapping.thread,items:[],next_cursor:null,resume_cursor:saved.resume_cursor,
  gaps:[],complete:false,source_exhausted:false,retry_pages:[]});
 // A complete same-version ledger resists downgrade. Change the test's inventory
 // hint by appending a new native item, as a real reader upgrade plus append does.
 await append([[137,'New reader catch-up']]);
 assert.equal((await run()).phase,'complete');assert.equal(transferred,before.length+58);
 await appendFile(path,message(138,'Native projection catches up'));
 const lagging=await run();assert.equal(lagging.phase,'partial');assert.equal(transferred,0);
 assert.ok(lagging.gap_count>0,'A source append cannot be skipped while its native projection is behind');
 await project([[138,'Native projection catches up']]);
 assert.equal((await run()).phase,'complete');assert.equal(transferred,1);
 const intact=await readFile(path);
 await writeFile(path,intact.toString().replace('Original 1','Mutated! 1'));
 const rewritten=await run();assert.equal(rewritten.phase,'partial');assert.equal(transferred,0);
 assert.ok(rewritten.gap_count>0,'An edit before the final page cannot be certified by a tail cursor');
 await writeFile(path,intact);assert.equal((await run()).phase,'complete');assert.equal(transferred,0);
 await writeFile(path,intact.subarray(0,Math.floor(intact.length/2)));
 assert.equal((await run()).phase,'partial');assert.equal(transferred,0);
 await writeFile(path,intact);assert.equal((await run()).phase,'complete');
 const final=await entries();assert.equal(final.length,before.length+60);
 assert.equal((await app.session.inspect(BACKGROUND_CONTEXT)).tasks.length,0);
}finally{await app.session.close(BACKGROUND_CONTEXT);input.close()}
