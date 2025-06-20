const { connect } = require('./index.js');

async function test() {
  try {
    console.log('Creating GlareDB session...');
    const session = connect();
    
    console.log('Running test query...');
    const result = await session.sql('SELECT 1 as test_column');
    
    console.log('Query result:');
    console.log(result.toString());
    
    console.log('Closing session...');
    session.close();
    
    console.log('Test completed successfully!');
  } catch (error) {
    console.error('Test failed:', error);
    process.exit(1);
  }
}

test();
