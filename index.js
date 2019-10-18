'use strict'

const program = require('commander')
const fs = require('fs').promises
const Promise = require('bluebird')
const { Client } = require('@elastic/elasticsearch')
const util = require('util');
const randomString = require('randomstring')

function hosts(value, previous) {
  return previous.concat([value]);
}

program
    .option('-f, --file <file>', 'path to the input json file')
    .option('-r, --records <number>', 'records to use per bulk index', 500, parseInt)
    .option('-c, --concurrency <number>', 'number of concurrent index requests', 10, parseInt)
    .option('-h, --host <hostname>', 'ES host', hosts, [])
    .option('-i, --index <index>', `ES index to index docs to. Ignored and used as default if --type-field option is present but the field doesn't exist for that doc.`)
    .option('-d, --delete-indices', 'First delete all indices associated with all documents')
    .option('-b, --dump-bulks <path>', 'Dump bulk index to directory')
    .option('-t, --type-field <field>', 'Type field from doc to be used as index name. This will create a separate index for each value for that field' )
    .option('-a, --alias <alias>', 'Create an alias for all the indices created using the -t option.')
    .option('-p, --index-postfix', 'Autogenerate an index postfix')
    .parse(process.argv)
    .version('0.0.1')

if (program.file === undefined) {
    process.exit()
}
const docs = require(program.file)
const esHost = program.host
const esIndex = program.index
const typeField = program.typeField
const aliasName = program.alias
const deleteFirst = program.deleteIndices
const dumpPath = program.dumpBulks
const recordsPerBulk = program.records
const concurrency = program.concurrency
const totalBulkRequests = Math.ceil(docs.length / recordsPerBulk)
const indexPostfix = program.indexPostfix ? `_${randomString.generate({length: 6, capitalization: 'lowercase'})}` : ''

const generatePostfix = () => {
    return Math.floor(Math.random() * Math.floor(
    u));
}

const defaultBulkIndexStatement = {
    index: {
        _index: esIndex + indexPostfix, 
        _type: '_doc'
    }
}
const defaultIndexSettings =  { 
    settings: {
        index: {
            refresh_interval: -1,
            number_of_replicas: 0,
            store: {
                type: "niofs",
            }//,
            //translog: {
            //    sync_interval: "50s",
            //    durability: "async",
            //    flush_threshold_size: "5gb",
            //}
        }   
    }
}
const endSettings = {
    index: {
        refresh_interval: null
    }
}

let startBulk

console.log("Run Parameters")
console.log("------------------------")
console.log("Indexes per request: ", recordsPerBulk)
console.log("Total number of requests: ", totalBulkRequests)
console.log("concurrent bulk requests: ", concurrency)
console.log("Num docs: ", docs.length)
console.log("ES Host: ", esHost)
if (typeField != undefined) {
    console.log(`Creating index for every value in ${typeField} field`)
} else {
    console.log(`Indexing docs into one index: ${esIndex}`)
}

// set up ES
const client = new Client({ node: esHost })

// creates the requisite indices in ES
const indexSetup = (indices) => {
    if (typeField) {
        if (deleteFirst) {
            console.log(`Regenerating ${indices.length} indices`)
    
            return Promise.each(indices, (index) => {
		    console.log(`Deleting index ${index}`)
                return client.indices.delete({index, ignoreUnavailable: true})
            })
            .then(() => {
		    console.log(`Creating ${indices.length} indices`)
                let startIndexCreate = new Date()
                return Promise.each(indices, (index) => {
			console.log(`creating ${index}`)
                    return client.indices.create({
                            index,
                            body: defaultIndexSettings
                        })
                    })
                    .then(() => {
                        const stopIndexCreate = new Date()
                        const totalIndexTimeMS = stopIndexCreate - startIndexCreate
                        const totalIndexTimeS = totalIndexTimeMS / 1000
                        console.log(`⏱  Index creation took: ${totalIndexTimeS}s`)
                    })
                })
        } else {
            console.log(`Creating ${indices.length} indices`)
    
            return Promise.each(indices, (index) => {
                return client.indices.create({
                    index,
                    body: defaultIndexSettings
                })
            })
        }
    } else if (deleteFirst) {
        console.log(esIndex)
        return client.indices.delete({index: esIndex, ignoreUnavailable: true})
            .then(() => client.indices.create({
                index: esIndex,
                body: defaultIndexSettings
            }))
    }
}

let numDocs = 0
// Put each document into a bulk bucket with the required insert operations
const createBuckets = () => {
    return new Promise((resolve, reject) => {
        let bulks = []
        let indices = {}

        const createIndexStatement = (doc) => {
            if (typeField) {
                const typeFieldValue = doc[typeField]
                if (typeFieldValue === undefined) {
                    console.log("Unknown eventType. Using default.")
                    return defaultBulkIndexStatement
                }
           
		const indexName = `${typeFieldValue}${indexPostfix}`
                indices[indexName] = {}
            
                const indexStatement = JSON.parse(JSON.stringify(defaultBulkIndexStatement))
                indexStatement.index._index = indexName

                return indexStatement
            } else {
                return defaultBulkIndexStatement
            }
        }

        for(let i = 0; i < totalBulkRequests; i++) {
            const bucketSlice = docs.slice(i * recordsPerBulk, (i + 1) * recordsPerBulk)
            const bucket = bucketSlice.flatMap((doc, index, array) => {
                    const indexStatement = createIndexStatement(doc)
                    return [indexStatement, doc]
                })

            //process.stdout.write("-")
            numDocs = numDocs + bucket.length
		bulks.push(bucket)
        }
        
        resolve([bulks, Object.keys(indices)])
    })
}

// create the bulk requests
const doBulk = (group, esClient) => {
    return Promise.each(group, (body) => {
        return esClient.bulk({
            body,
            refresh: 'false'
        })
        .then((res) => {
		console.log(res)
	    if (res.body.errors) {
		const items = res.body.items
		items.forEach((item) => {
			console.log(item)
		})
            }
            process.stdout.write(".")
        })
        .catch((e) => {
            console.log('FAILED')
            console.log(e)
            process.exit()
        })
    }).then(() => { process.stdout.write("!") })
}

// generate a queue of ES requests so that queues can be run individually serial, but in parallel to one another
const generateRequestQueues = (bulks) => {
    const bulkGroups = []
    const numInGroup = Math.ceil(bulks.length / concurrency)

    console.log("Total number of docs: ", docs.length)
    console.log("Total number of bulk requests: ", bulks.length)

    for(let i = 0; bulks.length > 0; i++) {
        const spliced = bulks.splice(0, numInGroup)
        bulkGroups.push(spliced)
    }

    console.log("Created %i queues of approximately %i bulk requests each", concurrency, numInGroup)

    process.stdout.write("Bulk indexing")
    startBulk = new Date()

    const promises = []
    bulkGroups.forEach((group) => {
        promises.push(doBulk(group, client))
    })

    return promises
}

const doDump = (bulks, indices) => {
	console.log(`Total docs: ${numDocs}`)
    if (dumpPath) {
        console.log(`Dumping bulk files to ${dumpPath}`)

        const files = []
        for (let i = 0; i < bulks.length; i++) {
            const filename = `${dumpPath}/bulk.${i}.out`
            const bulk = bulks[i]
            files.push(fs.writeFile(filename, util.inspect(bulk)))
        }
        
        return Promise.all(files)
            .then(() => {
                return [bulks, indices]
            })
    } else {
        return [bulks, indices]
    }
}

const makeSearchable = (indices) => {
    console.log('Making indices searchable')
    const eaches = Promise.each(indices, (index) => {
	console.log(`${index} is searchable`)
        client.indices.putSettings({
            index,
            body: endSettings
	})
    })
    return [indices, eaches]
}

const createAlias = (indices) => {
    if (aliasName && typeField) {
        console.log(`Creating alias ${aliasName} with ${indices.length} indexes`)    
	    const index = indices.join(',')
        return client.indices.putAlias({index, name: aliasName})
    }

	return
}

createBuckets()
    .spread(doDump)
    .spread((bulks, indices) => {
        return [bulks, indices, indexSetup(indices),]
    })
    .spread((bulks, indices) => {
        const requests = generateRequestQueues(bulks)
        return Promise.all(requests).then(() => { return indices })
    })
    .then((indices) => {
        const stopBulk = new Date()
        const totalBulkTimeMS = stopBulk - startBulk
        const totalBulkTimeS = totalBulkTimeMS / 1000
        console.log(`\n⏱  Bulk indexing took: ${totalBulkTimeS}`)

        return indices
    })
    .then(makeSearchable)
    .spread(createAlias)
