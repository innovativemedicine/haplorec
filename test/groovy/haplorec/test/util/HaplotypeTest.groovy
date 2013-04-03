package haplorec.test.util

import haplorec.util.Haplotype

import groovy.sql.Sql;
import groovy.util.GroovyTestCase

public class HaplotypeTest extends GroovyTestCase {
	
	def TEST_DB = "haplorec_test"
	def TEST_PORT = 3306
	def TEST_URL = "jdbc:mysql://localhost:${TEST_PORT}"
	def TEST_USER = "root"
	def TEST_PASSWORD = ""
	def TEST_DRIVER = "com.mysql.jdbc.Driver"

    Sql sql

    void setUp() {
        sql = Sql.newInstance(TEST_URL, TEST_USER, TEST_PASSWORD)
        // sql = Sql.newInstance(TEST_URL, TEST_USER, TEST_PASSWORD, TEST_DRIVER)
        assert sql != null
        // groovy.sql.Sql is doing some miserably weird garbage behind the scenes when calling 
        // Sql.execute(GString sql); in particular the error message suggests it's replacing 
        // ${SOMEVAR} with 'blah' (single quotes _included_), hence our explicit toString call.
		sql.execute "drop database if exists ${TEST_DB}".toString()
		sql.execute "create database ${TEST_DB}".toString()
        sql.close()
        sql = Sql.newInstance("${TEST_URL}/${TEST_DB}", TEST_USER, TEST_PASSWORD)
    }

    void tearDown() {
        sql.execute "drop database ${TEST_DB}".toString()
        sql.close()
    }

    void testGenePhenotypeToDrugRecommendation() {
    }

    void testSnpToGeneHaplotype() {
    }

    void testGenotypeToDrugRecommendation() {
    }

    def groupedRowsToColumnsTest(Map kwargs, ACols, ARows, BCols, BRows, groupBy, columnMap) {
        try {

            // setup
			def createTable = { table, cols -> sql.execute "create table ${table}(${cols.collect { c -> c + ' integer'}.join(', ')})".toString() }
			createTable('A', ACols)
			createTable('B', BCols)
            def insertSql = { table, columns, rows ->
                sql.execute """
                insert into ${table}(${columns.join(', ')}) values
                ${rows.collect { '(' + it.join(', ') + ')' }.join(', ')}
                """.toString()
            }
            insertSql('A', ACols, ARows)
            // test
            Haplotype.groupedRowsToColumns(sql, 'A', 'B', groupBy, columnMap, orderRowsBy: kwargs.orderRowsBy)
            assertEquals(BRows, sql.rows('select * from B').collect { r -> BCols.collect { r[it] } })
        } finally {
            // teardown
            sql.execute "drop table A".toString()
            sql.execute "drop table B".toString()
        }
    }
	
	def testGroupedRowsToColumns() {
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
	}

}
