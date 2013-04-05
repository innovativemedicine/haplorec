package haplorec.test.util

import haplorec.util.Sql

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
            insertSql(sql, 'A', ACols, ARows)
            // test
			List badGroups = []
            Sql.groupedRowsToColumns(sql, 'A', 'B', groupBy, columnMap, orderRowsBy: kwargs.orderRowsBy, badGroup: { g -> badGroups.add(g) })

			def hashRowsToListRows = { rows, cols -> 
				rows.collect { r -> 
					cols.collect { r[it] } 
				} 
			}
            assertEquals(BRows, selectSql(sql, 'B', BCols))
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
            insertSql(sql, 'existing_table', ['x', 'y', 'z'], existingRows)
            Sql.createTableFromExisting(kwargs, sql, 'new_table', kwargs.saveAs)
			log.info("new_table: ${sql.rows("show create table new_table")}")
			assertEquals(selectSql(sql, 'existing_table', columns), selectSql(sql, 'new_table', columns))
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

    def selectWhereSetContainsTest(Map kwargs = [:], singlesetTableCreateStmt, singlesetTableRows, multisetTableCreateStmt, multisetTableRows, expectRows) {
		def (singlesetTable, singlesetColumns) = parseCreateTableStatement(singlesetTableCreateStmt)
		def (multisetTable, multisetColumns) = parseCreateTableStatement(multisetTableCreateStmt)
		try {
	        tableTest(sql, 
	            [
	                [singlesetTableCreateStmt, singlesetTableRows], 
	                [multisetTableCreateStmt, multisetTableRows],
	            ]
	        ) {
				def nonsetColumns = multisetColumns.grep { m -> ! singlesetColumns.any { s -> m == s } }
	            Sql.selectWhereSetContains(sql, singlesetTable, multisetTable, singlesetColumns, nonsetColumns, 'C')
	            assertEquals(expectRows, selectSql(sql, 'C', nonsetColumns))
	        }
		} finally {
			sql.execute "drop table if exists C"
		}
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
            [
                [10],
                [20],
                [30],
            ])
		selectWhereSetContainsTest(
			// { (a, b) }
			"create table A(a varchar(10), b varchar(5))",
			[
				['10_chars__', '5ch_1'],
				['10_chars__', '5ch_2'],
				['10_chars__', '5ch_3'],
				['10_chars__', '5ch_4'],
			],
			// c, d, { (a, b) }
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
			[
				['10', '10_'],
				['20', '20_'],
				['30', '30_'],
			])
	    }

}
