#!/usr/bin/env node

import { WvletCompiler } from '../dist/index.js';

async function main() {
  console.log('Wvlet Compiler Example\n');
  
  const compiler = new WvletCompiler();
  
  // Get compiler version
  console.log(`Compiler version: ${WvletCompiler.getVersion()}\n`);
  
  // Example queries
  const queries = [
    'from users select name, email',
    'from orders where total > 100 select order_id, total',
    'from products select * limit 10',
    `from users u
     join orders o on u.id = o.user_id
     select u.name, count(*) as order_count
     group by u.name`
  ];
  
  for (const query of queries) {
    console.log('Wvlet Query:');
    console.log(query);
    console.log('\nCompiled SQL:');
    
    try {
      const sql = await compiler.compile(query);
      console.log(sql);
    } catch (error) {
      console.error('Compilation error:', error.message);
      if (error.location) {
        console.error(`  at line ${error.location.line}, column ${error.location.column}`);
      }
    }
    
    console.log('\n' + '-'.repeat(60) + '\n');
  }
  
  // Example with error handling
  console.log('Example with syntax error:');
  const invalidQuery = 'this is not valid wvlet syntax';
  console.log(invalidQuery);
  
  try {
    await compiler.compile(invalidQuery);
  } catch (error) {
    console.error('\nExpected error:', error.message);
  }
}

main().catch(console.error);