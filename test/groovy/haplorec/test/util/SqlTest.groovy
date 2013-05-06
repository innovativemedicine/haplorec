package haplorec.test.util

import java.util.Map;

import haplorec.util.Sql

import groovy.lang.Closure;
import groovy.util.GroovyTestCase

public class SqlTest extends DBTest {
	
	def TEST_DB = "haplorec_test"
	def TEST_HOST = "localhost"
	def TEST_PORT = 3306
	def TEST_USER = "root"
	def TEST_PASSWORD = ""

    def sql

    void setUp() {
        sql = setUpDB(TEST_DB,
                      host:TEST_HOST,
                      user:TEST_USER,
                      password:TEST_PASSWORD,
                      port:TEST_PORT)
    }

    void tearDown() {
        tearDownDB(TEST_DB, sql)
    }

    def groupedRowsToColumnsTest(Map kwargs = [:], ACols, ARows, BCols, BRows, groupBy, columnMap) {
        try {
            // setup
			def createTable = { table, cols -> sql.execute "create table ${table}(${cols.collect { c -> c + ' integer'}.join(', ')})".toString() }
			createTable('A', ACols)
			createTable('B', BCols)
            insert(sql, 'A', ACols, ARows)
            // test
			List badGroups = []
            Sql.groupedRowsToColumns(sql, 'A', 'B', groupBy, columnMap, orderRowsBy: kwargs.orderRowsBy, badGroup: { g -> badGroups.add(g) })

			def hashRowsToListRows = { rows, cols -> 
				rows.collect { r -> 
					cols.collect { r[it] } 
				} 
			}
            assertEquals(BRows, select(sql, 'B', BCols))
			if (kwargs.badGroups != null) {
				def expect = kwargs.badGroups
				def got = badGroups.collect { g -> hashRowsToListRows(g, ACols) }
				assertEquals(expect, got)
			}
        } finally {
            // teardown
            sql.execute "drop table A".toString()
            sql.execute "drop table B".toString()
        }
    }
	
	void testGroupedRowsToColumns() {
		def ACols = ['x', 'y']
		def BCols = ['x', 'y1', 'y2']
		def groupBy = 'x'
		def columnMap = ['x':'x', 'y':['y1', 'y2']]
		groupedRowsToColumnsTest(
			ACols,
			[
				[1, 2],
				[1, 3],
			],
			BCols, 
			[
				[1, 2, 3],
			], groupBy, columnMap)
		groupedRowsToColumnsTest(
			ACols,
			[
				[1, 2],
			],
			BCols,
			[
				[1, 2, null],
			], groupBy, columnMap)
		groupedRowsToColumnsTest(
			ACols,
			[
				[1, 2],
			],
			BCols,
			[
				[1, null, 2],
			], groupBy,
			// fill 'y2' before filling 'y1', so that a group in A less than size two (that is, size 1) will make 'y1' null over 'y2' 
			['x':'x', 'y':['y2', 'y1']])
		groupedRowsToColumnsTest(
			ACols,
			[
				// without orderRowsBy: ['y'], we get [1, 3, 2] for the B row
				[1, 3],
				[1, 2],
			],
			BCols,
			[
				[1, 3, 2],
			], groupBy, columnMap)
		groupedRowsToColumnsTest(
			ACols,
			[
				// with orderRowsBy: ['y'], we get [1, 2, 3] for the B row
				[1, 3],
				[1, 2],
			],
			BCols,
			[
				[1, 2, 3],
			], groupBy, columnMap, orderRowsBy:['y'])
		groupedRowsToColumnsTest(
			ACols,
			[
				// multiple groups with varying size
				[1, 2],
				[1, 3],
				
				[5, 6],
				[5, 7],
				
				[8, 9],
			],
			BCols,
			[
				[1, 2, 3],
				[5, 6, 7],
				[8, 9, null],
			], groupBy, columnMap)
		// error cases
		groupedRowsToColumnsTest(
			ACols,
			[
				[1, 1],
				[1, 2],
				[1, 3],
			],
			BCols,
			[
			],
			badGroups:[
				[
					[1, 1],
					[1, 2],
					[1, 3],
				],
			],
		 	groupBy, columnMap)
		// empty input table
		groupedRowsToColumnsTest(
			ACols,
			[
			],
			BCols,
			[
			],
			badGroups:[
			],
			groupBy, columnMap)
	}

    def createTableFromExistingTest(Map kwargs = [:], existingRows, columns = null) {
        if (kwargs.saveAs == null) { kwargs.saveAs = 'MyISAM' }
		kwargs.existingTable = 'existing_table'
		if (columns == null) { columns = kwargs.columns }
        try {
            sql.execute "create table existing_table(x integer, y varchar(20), z double)"
            insert(sql, 'existing_table', ['x', 'y', 'z'], existingRows)
            Sql.createTableFromExisting(kwargs, sql, 'new_table')
			log.info("new_table: ${sql.rows("show create table new_table")}")
			assertEquals(select(sql, 'existing_table', columns), select(sql, 'new_table', columns))
        } finally {
            sql.execute "drop table if exists existing_table"
			sql.execute "drop table if exists new_table"
        }
    }

    void testCreateTableFromExisting() {
		def existingRows = [
            [1, 'hello', 1.0],
            [2, 'there', 2.0],
            [3, 'world', 3.0],
        ]
        createTableFromExistingTest(columns:['x'], indexColumns:['x'], existingRows)
		createTableFromExistingTest(columns:['x', 'y'], indexColumns:[['x'], ['x', 'y']], existingRows)
    }

	def _setContainsTest(Map kwargs = [:], tableACreateStmt, tableARows, tableBCreateStmt, tableBRows, expectRows, f) {
		def (tableA, tableAColumns) = parseCreateTableStatement(tableACreateStmt)
		def (tableB, tableBColumns) = parseCreateTableStatement(tableBCreateStmt)
		try {
			tableTest(sql,
				[
					[tableACreateStmt, tableARows],
					[tableBCreateStmt, tableBRows],
				]
			) {
				def setColumns = tableBColumns.grep { b -> tableAColumns.any { a -> b == a } }
				f(kwargs + [intoTable:'C'], sql, tableA, tableB, setColumns)
				assertEquals(expectRows.sort(), select(sql, 'C', kwargs.selectAnswer ?: kwargs.select).sort())
			}
		} finally {
			sql.execute "drop table if exists C"
		}
	}
	
	def selectWhereEitherSetContainsTest(Map kwargs = [:], tableACreateStmt, tableARows, tableBCreateStmt, tableBRows, expectRows) {
        _setContainsTest(kwargs, tableACreateStmt, tableARows, tableBCreateStmt, tableBRows, expectRows, Sql.&selectWhereEitherSetContains)
    }

	def selectWhereSetContainsTest(Map kwargs = [:], tableACreateStmt, tableARows, tableBCreateStmt, tableBRows, expectRows) {
        _setContainsTest(kwargs, tableACreateStmt, tableARows, tableBCreateStmt, tableBRows, expectRows, Sql.&selectWhereSetContains)
    }
	
	void testSelectWhereSetContains() {

		selectWhereSetContainsTest(
			// { (x, y) }
			"create table A(x integer, y integer)",
			[
				[1, 1],
				[1, 2],
				[1, 3],
				[1, 4],
			],
			// z, { (x, y) }
			tableBGroupBy: ['z'],
			"create table B(z integer, x integer, y integer)",
			[
				// A == B
				[10, 1, 1],
				[10, 1, 2],
				[10, 1, 3],
				[10, 1, 4],
				// A <= B (ignored)
				[20, 1, 1],
				[20, 1, 2],
				[20, 1, 3],
				// A >= B
				[30, 1, 1],
				[30, 1, 2],
				[30, 1, 3],
				[30, 1, 4],
				[30, 1, 5],
				// nonzero intersection, but neither subset nor superset
				[40, 1, 1],
				[40, 1, 5],
			],
			select: ['z'],
			[
				[10],
				[30],
			])


		selectWhereSetContainsTest(
			// a, b, { (x, y) }
			tableAGroupBy: ['a', 'b'],
			"create table A(a integer, b integer, x integer, y integer)",
			[
				[11, 12, 1, 1],
				[11, 12, 1, 2],
				[11, 12, 1, 3],
				[11, 12, 1, 4],

				[13, 14, 1, 1],
				[13, 14, 1, 2],
				[13, 14, 1, 3],
				[13, 14, 1, 4],
			],
			// z, { (x, y) }
			tableBGroupBy: ['z'],
			"create table B(z integer, x integer, y integer)",
			[
				// A == B
				[10, 1, 1],
				[10, 1, 2],
				[10, 1, 3],
				[10, 1, 4],
				// A <= B (ignored)
				[20, 1, 1],
				[20, 1, 2],
				[20, 1, 3],
				// A >= B
				[30, 1, 1],
				[30, 1, 2],
				[30, 1, 3],
				[30, 1, 4],
				[30, 1, 5],
				// nonzero intersection, but neither subset nor superset
				[40, 1, 1],
				[40, 1, 5],
			],
			// a, b, z, { (x, y) }
			select: ['a', 'b', 'z'],
			[
				[11, 12, 10],
				[11, 12, 30],

				[13, 14, 10],
				[13, 14, 30],
			])
		
    }

	void testSelectWhereEitherSetContains() {

		selectWhereEitherSetContainsTest(
			// { (x, y) }
			"create table A(x integer, y integer)",
			[
				[1, 1],
				[1, 2],
				[1, 3],
				[1, 4],
			],
			// z, { (x, y) }
			tableBGroupBy: ['z'],
			"create table B(z integer, x integer, y integer)",
			[
				// equal set
				[10, 1, 1],
				[10, 1, 2],
				[10, 1, 3],
				[10, 1, 4],
				// subset
				[20, 1, 1],
				[20, 1, 2],
				[20, 1, 3],
				// superset
				[30, 1, 1],
				[30, 1, 2],
				[30, 1, 3],
				[30, 1, 4],
				[30, 1, 5],
				// nonzero intersection, but neither subset nor superset
				[40, 1, 1],
				[40, 1, 5],
			],
			select: ['z'],
			[
				[10],
				[20],
				[30],
			])

		selectWhereEitherSetContainsTest(
			// { (a, b) }
			"create table A(a varchar(10), b varchar(5))",
			[
				['10_chars__', '5ch_1'],
				['10_chars__', '5ch_2'],
				['10_chars__', '5ch_3'],
				['10_chars__', '5ch_4'],
			],
			// c, d, { (a, b) }
			tableBGroupBy: ['c', 'd'],
			"create table B(c varchar(10), d varchar(5), a varchar(10), b varchar(5))",
			[
				// equal set
				['10', '10_', '10_chars__', '5ch_1'],
				['10', '10_', '10_chars__', '5ch_2'],
				['10', '10_', '10_chars__', '5ch_3'],
				['10', '10_', '10_chars__', '5ch_4'],
				// subset
				['20', '20_', '10_chars__', '5ch_1'],
				['20', '20_', '10_chars__', '5ch_2'],
				['20', '20_', '10_chars__', '5ch_3'],
				// superset
				['30', '30_', '10_chars__', '5ch_1'],
				['30', '30_', '10_chars__', '5ch_2'],
				['30', '30_', '10_chars__', '5ch_3'],
				['30', '30_', '10_chars__', '5ch_4'],
				['30', '30_', '10_chars__', '5ch_5'],
				// nonzero intersection, but neither subset nor superset
				['40', '40_', '10_chars__', '5ch_1'],
				['40', '40_', '10_chars__', '5ch_5'],
			],
			select: ['c', 'd'],	
			[
				['10', '10_'],
				['20', '20_'],
				['30', '30_'],
			])

		selectWhereEitherSetContainsTest(
			// a, b, { (x, y) }
			tableAGroupBy: ['a', 'b'],
			"create table A(a integer, b integer, x integer, y integer)",
			[
				[11, 12, 1, 1],
				[11, 12, 1, 2],
				[11, 12, 1, 3],
				[11, 12, 1, 4],

				[13, 14, 1, 1],
				[13, 14, 1, 2],
				[13, 14, 1, 3],
				[13, 14, 1, 4],
			],
			// z, { (x, y) }
			tableBGroupBy: ['z'],
			"create table B(z integer, x integer, y integer)",
			[
				// equal set
				[10, 1, 1],
				[10, 1, 2],
				[10, 1, 3],
				[10, 1, 4],
				// subset
				[20, 1, 1],
				[20, 1, 2],
				[20, 1, 3],
				// superset
				[30, 1, 1],
				[30, 1, 2],
				[30, 1, 3],
				[30, 1, 4],
				[30, 1, 5],
				// nonzero intersection, but neither subset nor superset
				[40, 1, 1],
				[40, 1, 5],
			],
			// a, b, z, { (x, y) }
			select: ['a', 'b', 'z'],
			[
				[11, 12, 10],
				[11, 12, 20],
				[11, 12, 30],

				[13, 14, 10],
				[13, 14, 20],
				[13, 14, 30],
			])
		
		selectWhereEitherSetContainsTest(
			sqlParams:[somevar:55],
			// { (x, y) }
			"create table A(x integer, y integer)",
			[
				[1, 1],
				[1, 2],
				[1, 3],
				[1, 4],
			],
			// z, { (x, y) }
			tableBGroupBy: ['z'],
			"create table B(z integer, x integer, y integer)",
			[
				// equal set
				[10, 1, 1],
				[10, 1, 2],
				[10, 1, 3],
				[10, 1, 4],
				// subset
				[20, 1, 1],
				[20, 1, 2],
				[20, 1, 3],
				// superset
				[30, 1, 1],
				[30, 1, 2],
				[30, 1, 3],
				[30, 1, 4],
				[30, 1, 5],
				// nonzero intersection, but neither subset nor superset
				[40, 1, 1],
				[40, 1, 5],
			],
			select: [':somevar'],
			selectAnswer: ['55'],
			[
				[55],
			])

	}
	
	def withUniqueIdTest(Map kwargs = [:], tableCreateStmt) {
		def (table, _) = parseCreateTableStatement(tableCreateStmt)
		tableTest(sql, [
			[tableCreateStmt],
		]) {
			/* expected behaviour of Sql.withUniqueId:
			 * - a new row exists in $table with a unique id during the execution of doWithId
			 * - after withUniqueId (and doWithId) executes, that same row has been removed
			 */
			def id
			assert 0 == selectCount(sql, table)
			Sql.withUniqueId(kwargs, sql, table) { _id ->
				id = _id
				assert 1 == selectCount(sql, table) : "a new row exists in $table with a unique id during the execution of doWithId"
			}
			assert 0 == selectCount(sql, table) : "after withUniqueId (and doWithId) executes, that same row has been removed"
		}
	}
	
	void testWithUniqueId() {
		withUniqueIdTest("create table id_generator(id bigint not null auto_increment, primary key (id))")
		withUniqueIdTest("""\
            create table id_generator(
                id bigint not null auto_increment, 
			    somestring varchar(50) not null, 
			    primary key (id))
			""",
			values: ['somestring':'somevalue'])
	}

}
