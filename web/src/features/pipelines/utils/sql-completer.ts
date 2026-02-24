import 'ace-builds/src-noconflict/ext-language_tools'

/**
 * Creates a custom SQL completer that handles dynamic aliases, destination tables, and source tables.
 *
 * @param schema - Schema for source tables (table_name -> columns)
 * @param fetchDestinationSchema - Async function to fetch schema for a specific destination table
 * @param fetchSourceSchema - Async function to fetch schema for a specific source table
 * @param destinationName - Name of the destination (used for pg_<dest_name> prefix)
 * @param sourceName - Name of the source (used for pg_src_<source_name> prefix)
 * @param onLoading - Callback to indicate loading state (true/false)
 */
export const createSqlCompleter = (
  schema: Record<string, string[]>,
  fetchDestinationSchema: (tableName: string) => Promise<string[]>,
  fetchSourceSchema: (tableName: string) => Promise<string[]>,
  destinationName?: string,
  sourceName?: string,
  onLoading?: (isLoading: boolean) => void
) => {
  // Local cache for destination and source tables to avoid repeated API calls
  const destinationCache: Record<string, string[]> = {}
  const sourceCache: Record<string, string[]> = {}

  return {
    getCompletions: async (
      editor: any,
      session: any,
      pos: any,
      prefix: any,
      callback: any
    ) => {
      const line = session.getLine(pos.row)
      const textBeforeCursor = line.substring(0, pos.column)

      const dotMatch = textBeforeCursor.match(
        /([a-zA-Z0-9_\-]+(?:\.[a-zA-Z0-9_\-]+)*)\.\s*$/
      )

      if (dotMatch) {
        const identifierChain = dotMatch[1].toLowerCase()
        const fullSql = editor.getValue()

        // 1. Parse Aliases with awareness of SQL structure
        const aliasMap: Record<string, string> = {}

        // Remove comments to avoid false positives
        const cleanSql = fullSql
          .replace(/--.*$/gm, '')
          .replace(/\/\*[\s\S]*?\*\//g, '')

        // Keywords that usually precede a table reference
        // Simplified and more robust regex
        const tableRefRegex =
          /\b(FROM|JOIN|UPDATE|INTO|USING)\s+([a-zA-Z0-9_.\-]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?\b/gi

        let m
        while ((m = tableRefRegex.exec(cleanSql)) !== null) {
          const tableName = m[2].toLowerCase()
          const aliasName = m[3] ? m[3].toLowerCase() : null

          if (aliasName) {
            // Check if alias is a keyword
            const isKeyword = [
              'select',
              'from',
              'join',
              'where',
              'as',
              'on',
              'and',
              'or',
              'group',
              'order',
              'limit',
              'left',
              'right',
              'inner',
              'outer',
              'using',
              'update',
              'set',
              'insert',
              'into',
            ].includes(aliasName)
            if (!isKeyword) {
              aliasMap[aliasName] = tableName
            }
          }
        }

        const destPrefix = destinationName
          ? `pg_${destinationName.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`
          : 'pg_dest'
        const srcPrefix = sourceName
          ? `pg_src_${sourceName.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`
          : 'pg_src'

        // Handle destination prefix (pg_<dest_name>)
        if (identifierChain === destPrefix) {
          let tables: string[] = []
          if (destinationCache['__tables__']) {
            tables = destinationCache['__tables__']
          } else {
            try {
              if (onLoading) onLoading(true)
              tables = await fetchDestinationSchema('')
              if (tables.length > 0) {
                destinationCache['__tables__'] = tables
              }
            } catch (e) {
            } finally {
              if (onLoading) onLoading(false)
            }
          }

          return callback(
            null,
            tables.map((t) => ({
              caption: t,
              value: t,
              meta: 'dest table',
              score: 2000,
            }))
          )
        }

        // Handle source prefix (pg_src_<source_name>)
        if (identifierChain === srcPrefix) {
          let tables: string[] = []
          if (sourceCache['__tables__']) {
            tables = sourceCache['__tables__']
          } else {
            try {
              if (onLoading) onLoading(true)
              tables = await fetchSourceSchema('')
              if (tables.length > 0) {
                sourceCache['__tables__'] = tables
              }
            } catch (e) {
            } finally {
              if (onLoading) onLoading(false)
            }
          }

          return callback(
            null,
            tables.map((t) => ({
              caption: t,
              value: t,
              meta: 'source table',
              score: 2000,
            }))
          )
        }

        // Handle destination table columns (pg_<dest_name>.<table>)
        if (identifierChain.startsWith(destPrefix + '.')) {
          const tableName = identifierChain.replace(destPrefix + '.', '')
          let columns: string[] = []

          if (destinationCache[tableName]) {
            columns = destinationCache[tableName]
          } else {
            try {
              if (onLoading) onLoading(true)
              columns = await fetchDestinationSchema(tableName)
              if (columns.length > 0) {
                destinationCache[tableName] = columns
              }
            } catch (e) {
              console.error('Fetch error:', e)
            } finally {
              if (onLoading) onLoading(false)
            }
          }

          return callback(
            null,
            columns.map((c) => ({
              caption: c,
              value: c,
              meta: 'dest column',
              score: 1000,
            }))
          )
        }

        // Handle source table columns (pg_src_<source_name>.<table>)
        if (identifierChain.startsWith(srcPrefix + '.')) {
          const tableName = identifierChain.replace(srcPrefix + '.', '')
          let columns: string[] = []

          if (sourceCache[tableName]) {
            columns = sourceCache[tableName]
          } else {
            try {
              if (onLoading) onLoading(true)
              columns = await fetchSourceSchema(tableName)
              if (columns.length > 0) {
                sourceCache[tableName] = columns
              }
            } catch (e) {
              console.error('Fetch error:', e)
            } finally {
              if (onLoading) onLoading(false)
            }
          }

          return callback(
            null,
            columns.map((c) => ({
              caption: c,
              value: c,
              meta: 'source column',
              score: 1000,
            }))
          )
        }

        // Handle aliases
        if (aliasMap[identifierChain]) {
          const realTableName = aliasMap[identifierChain]

          if (schema[realTableName]) {
            return callback(
              null,
              schema[realTableName].map((c) => ({
                caption: c,
                value: c,
                meta: 'column',
                score: 1000,
              }))
            )
          }

          // Is it a destination table alias?
          // e.g. realTableName = "pg_<dest>.tbl_sales_region"
          if (realTableName.startsWith(destPrefix + '.')) {
            const tableName = realTableName.replace(destPrefix + '.', '')
            let columns: string[] = []

            if (destinationCache[tableName]) {
              columns = destinationCache[tableName]
            } else {
              try {
                if (onLoading) onLoading(true)
                columns = await fetchDestinationSchema(tableName)
                if (columns.length > 0) {
                  destinationCache[tableName] = columns
                }
              } catch (e) {
                console.error('Fetch error:', e)
              } finally {
                if (onLoading) onLoading(false)
              }
            }

            return callback(
              null,
              columns.map((c) => ({
                caption: c,
                value: c,
                meta: 'dest column',
                score: 1000,
              }))
            )
          }

          // Is it a source table alias?
          // e.g. realTableName = "pg_src_<source>.ref_products"
          if (realTableName.startsWith(srcPrefix + '.')) {
            const tableName = realTableName.replace(srcPrefix + '.', '')
            let columns: string[] = []

            if (sourceCache[tableName]) {
              columns = sourceCache[tableName]
            } else {
              try {
                if (onLoading) onLoading(true)
                columns = await fetchSourceSchema(tableName)
                if (columns.length > 0) {
                  sourceCache[tableName] = columns
                }
              } catch (e) {
                console.error('Fetch error:', e)
              } finally {
                if (onLoading) onLoading(false)
              }
            }

            return callback(
              null,
              columns.map((c) => ({
                caption: c,
                value: c,
                meta: 'source column',
                score: 1000,
              }))
            )
          }
        }

        // Direct table reference
        if (schema[identifierChain]) {
          return callback(
            null,
            schema[identifierChain].map((c) => ({
              caption: c,
              value: c,
              meta: 'column',
              score: 1000,
            }))
          )
        }
      }

      // 3. Default Suggestions (Keywords only, cleaner)
      // Since we rely on standard keyword completer, we return nothing here unless we want to suggest table names at top level
      if (prefix.length >= 0) {
        // Only suggest source table names and database prefixes at basic level
        const suggestions = []

        // CDC Source Tables (from current table being synced)
        suggestions.push(
          ...Object.keys(schema).map((table) => ({
            caption: table,
            value: table,
            meta: 'cdc table',
            score: 500,
          }))
        )

        // Destination Prefix Hint
        if (destinationName) {
          const destPrefix = `pg_${destinationName.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`
          suggestions.push({
            caption: destPrefix,
            value: destPrefix,
            meta: 'destination',
            score: 600,
          })
        }

        // Source Prefix Hint
        if (sourceName) {
          const srcPrefix = `pg_src_${sourceName.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`
          suggestions.push({
            caption: srcPrefix,
            value: srcPrefix,
            meta: 'source db',
            score: 600,
          })
        }

        callback(null, suggestions)
      }
    },
  }
}
