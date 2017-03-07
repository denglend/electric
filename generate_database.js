var Promise = require('bluebird');
const stream = require('stream');
var fs = Promise.promisifyAll(require('fs'));
var csvparse = Promise.promisify(require('csv-parse'));
var csvstringify = Promise.promisify(require('csv-stringify'));
var merge = require('deepmerge');
var logger = require('./util/logger');
var secrets = require("./secrets");
logger.level = "verbose";

var pg = require("pg");
Object.keys(pg).forEach(function(key) {
	var Class = pg[key];
	if (typeof Class === "function") {
		Promise.promisifyAll(Class.prototype);
		Promise.promisifyAll(Class);
	}
});
Promise.promisifyAll(pg);
var pgCopyFrom = require('pg-copy-streams').from;

var db = new pg.Pool(secrets.dbconfig);

main();

async function main() {
	try {
		var DataJSONData = await fs.readFileAsync('data/data.json','utf8');
		logger.debug("PLACEHOLDER: Parse command line options");
		await ClearDatabase();
		await ParseDataFiles(DataJSONData);
		await Cont();
		await db.end();
	}
	catch (e){
		GenericAbortError(e);
	}
}
async function ClearDatabase() {
	var ClearDBSQL = "DROP SCHEMA public CASCADE; CREATE SCHEMA public AUTHORIZATION electric; GRANT ALL ON SCHEMA public TO electric; GRANT ALL ON SCHEMA public TO public; COMMENT ON SCHEMA public  IS 'standard public schema';";
	logger.debug("Clearing existing DB data");
	return db.query(ClearDBSQL);
}
async function ParseDataFiles(data) {
	var DataFileObj = JSON.parse(data);
	var OpenSet = [];													//names of data sets yet to parse
	DataFileObj.forEach(function(el) {OpenSet.push(el.name);});				//add all data sets to Open Set
	var ParseRound = 0;  var PrevOpenLen = -1;
	await CreateDatabaseTables(DataFileObj);
	await ParseDataFileNextRound();

	async function ParseDataFileNextRound() {
		var PromiseSet = [];
		if (PrevOpenLen == OpenSet.length) {						//if size of ClosedSet is the same two loops in a row, we are stuck
			throw("ERROR: Unreachable dependencies.\nOpenSet: "+OpenSet.join(", "));
		}
		else {
			PrevOpenLen = OpenSet.length;
			logger.verbose("Parsing Data File Round "+ParseRound);
			var FilteredDataFileObj = DataFileObj.filter(function(el) { 									//Get all objs for open data sets 
				return OpenSet.indexOf(el.name) > -1;
			}).filter(function(el) {										//... that don't have unfulfilled dependencies
				return 	el.dependencies === undefined ||	el.dependencies.length === 0 ||
						el.dependencies.filter(function(el) {return OpenSet.indexOf(el) != -1;}).length ===0;
			});
			for (let el of FilteredDataFileObj) {							//... parse them and update the open/closed set lists
				logger.verbose(" Loading",el.name);
				PromiseSet.push(LoadDataSetAsync(el));
				OpenSet.splice(OpenSet.indexOf(el.name),1);
			}
			ParseRound++;
			await Promise.all(PromiseSet);
			if (OpenSet.length == 0) return;
			else  await ParseDataFileNextRound();
		} 
	}
	async function CreateDatabaseTables(DataFileObj) {
		var PromiseSet = [];
		logger.debug("Creating Database Tables");
		for (let schema of DataFileObj) {
			PromiseSet.push(db.query("CREATE SEQUENCE "+schema.name+"id START 1; "));
			PromiseSet.push(db.query("CREATE SEQUENCE "+schema.name+"_akaid START 1; "));
			PromiseSet.push(db.query("CREATE SEQUENCE "+schema.name+"_metaid START 1; "));
		}
		await Promise.all(PromiseSet);
		PromiseSet = [];
		for (let schema of DataFileObj) {
			var CreateStr = "CREATE TABLE "+schema.name+" ( id integer PRIMARY KEY DEFAULT nextval('"+schema.name+"id') ";
			for (let column in schema.columns) {
				CreateStr += " , ";
				let CurColName = schema.columns[column].type=="link" ? column +'id' : column;
				let CurColType = "varchar(1000)";
				if (schema.columns[column].type =="link" || schema.columns[column].type=="int") CurColType = "integer";
				else if (schema.columns[column].type=="date") CurColType = "date";
				CreateStr += CurColName+" "+CurColType;
			}
			CreateStr +=");"; 
			logger.debug(" "+CreateStr,schema.name);
			PromiseSet.push(db.query(CreateStr));
			let CreateStrMeta = "CREATE TABLE "+schema.name+"_meta (id integer PRIMARY KEY DEFAULT nextval('"+schema.name+"_metaid'), type varchar(1000), val varchar(1000));";
			let CreateStrAka = "CREATE TABLE "+schema.name+"_aka (id integer PRIMARY KEY DEFAULT nextval('"+schema.name+"_akaid'), "+schema.name+"id int, val varchar(1000));";
			PromiseSet.push(db.query(CreateStrMeta));
			PromiseSet.push(db.query(CreateStrAka));

		}
		return Promise.all(PromiseSet);
	}
}

function Cont() {
	logger.silly("Continuing...");

}

function GenericAbortError(e) {
	logger.error(e);
	process.exitCode = 1;
}

async function LoadDataSetAsync(schema) {
	/*
		Load file names
		Add all other .json filenames to JSONOpenSet
		Add all .csv filenames to CSVOpenSet
		Read all JSON files
			if there is an all.json, read it and add values to el
			Add all other JSON files's data to individualized copies of el
		THEN
		ParseDataSubFolderNextRound()
			for each file in JSONOpenSet that has no unmatched dependences
				Read CSV file data
				THEN
				[somehow resolve AKAs and create links]
				THEN
				push to PostgreSQL database (using individualized copy of el)
				THEN
				Remove from CSV OpenSet and JSON OpenSet
			THEN
			if JSONOpenSet and CSVOpenSet are not empty, ParseDataSubFolderNextRound




	*/

	//var JSONOpenSet = [];
	var CSVOpenSet = [];
	var CSVClosedSet = [];
	var FileSchemas = {};
	var DataSubFolderRound = 0;
	var PrevOpenLen = -1;
	logger.debug("  Loading data for schema",schema.name);

	//logger.debug("Initial schema is:");
	//logger.debug(JSON.stringify(schema,null,4));
	var FileNameList = await fs.readdirAsync('data/'+schema.name);
	await SetupSchemas(FileNameList);
	await ParseDataSubFolderNextRound();
	return;

	async function SetupSchemas(filenames) {
		//Reads in all CSV and JSON files, to create the opensets; reads in JSON files and adds them to schema
		var PromiseSet = [];
		logger.debug("  Entering SetupSchemas. Found Files:",schema.name);
		filenames.forEach(function(fn) {
			if (fn.toLowerCase() == 'all.json') {
				logger.debug("   "+fn,schema.name);
				PromiseSet.push(fs.readFileAsync('data/'+schema.name+'/'+fn));
			}
			else if (fn.slice(-5).toLowerCase() == '.json' ) {
				logger.debug("   "+fn,schema.name);
				//JSONOpenSet.push(fn);
				PromiseSet.push(fs.readFileAsync('data/'+schema.name+'/'+fn));
			}
			else if (fn.slice(-4).toLowerCase() == '.csv') {
				logger.debug("   "+fn,schema.name);
				CSVOpenSet.push(fn.slice(0,-4));
			}
		});
		var JSONData = await Promise.all(PromiseSet);
		CreateFileSchemas(JSONData);
	}
	function CreateFileSchemas(JSONData) {
		//Extend default schema will all.json
		//Save the file schema (extended from default schema) for each JSON file found.
		//Copy default schema for each CSV file that does not have a json file
		//logger.debug("Entering CreateFileSchemas");
		JSONData = JSONData.map(JSON.parse);
		var DefaultSchemaData = JSONData.filter(function(el) {return el.name.toLowerCase() == "all" || el.name.toLowerCase() == schema.name.toLowerCase();});
		if (DefaultSchemaData.length ==1) {
			logger.debug("    Found all.json default schema data");
			schema = merge(schema,DefaultSchemaData[0],{clone:true});
			//logger.debug("Extending "+schema.name+" schema.  New schema: ");
			//logger.debug(JSON.stringify(schema,null,4));
		}
		else if (DefaultSchemaData.length >1) throw("    Found more than one default schema JSON file");
		for (let DataObj of JSONData.filter(function(el) {return el.name.toLowerCase() != "all" && el.name.toLowerCase() != schema.name.toLowerCase();})) {
			if (DataObj.name === undefined)  throw("    No name in JSON file - "+JSON.stringify(DataObj));
			FileSchemas[DataObj.name] = merge(schema,DataObj,{clone:true});
			for (let col in FileSchemas[DataObj.name].columns) { 
				//Add servesas to all columns that don't have them (so we can easily find the logical col name later)
				if (FileSchemas[DataObj.name].columns[col].servesas === undefined) FileSchemas[DataObj.name].columns[col].servesas = col;
			}
			//logger.debug("Read individual file schema for "+schema.name+"/"+DataObj.name+".  Schema is:");
			//logger.debug(JSON.stringify(FileSchemas[DataObj.name],null,4));
		}
		for (let CSVName of CSVOpenSet) {
			if (FileSchemas[CSVName] === undefined) {
				logger.debug("    No explicit schema file for so using default schema",schema.name+'.'+CSVName);
				FileSchemas[CSVName] = merge({},schema);
				for (let col in FileSchemas[CSVName].columns) { 
					//Add servesas to all columns that don't have them (so we can easily find the logical col name later)
					if (FileSchemas[CSVName].columns[col].servesas === undefined) FileSchemas[CSVName].columns[col].servesas = col;
				}
			}
		}

	}
	async function ParseDataSubFolderNextRound() {
		/*
			Filter schemas for ones that have no dependencies OR dependencies are fulfilled
				Load and process those CSVs
				Remove from openset and add to closed set
				If openset is not empty, loop
		*/
		logger.verbose("Data Subfolder Parsing Round "+DataSubFolderRound,schema.name);
		var PromiseSet = [];
		for (let CurDataSetName of CSVOpenSet) {
			let CurSchema = FileSchemas[CurDataSetName];
			logger.debug(" Dependencies: "+JSON.stringify(CurSchema.dependencies),schema.name+'.'+CurDataSetName);
			if (CurSchema.dependencies === undefined || CurSchema.dependencies.filter(function(d) { 
				return d.startsWith(schema.name+'.') && CSVClosedSet.indexOf(d.slice(schema.name.length+1)) == -1;
			}).length == 0) {
				logger.verbose(" Parsing ",schema.name+"."+CurDataSetName);
				PromiseSet.push(HandleCSVFile(CurDataSetName));	
			}
		}
		await Promise.all(PromiseSet);

		logger.debug(" After Round "+DataSubFolderRound,schema.name);
		logger.debug("  CSVOpenSet: "+JSON.stringify(CSVOpenSet),schema.name);
		logger.debug("  CSVClosedSet: "+JSON.stringify(CSVClosedSet),schema.name);
		if (CSVOpenSet.length ==0) {
			logger.debug("  ParseDataSubFolderNextRound CSVOpenSet length is 0, returning",schema.name);
			return;
		}
		else if (CSVOpenSet.length == PrevOpenLen) {
			throw(schema.name+" ParseDataSubFolderNextRound dependencies unreachable.  CSVOpenSet: "+JSON.stringify(CSVOpenSet));
		}
		else {
			PrevOpenLen = CSVOpenSet.length;
			DataSubFolderRound++;
			await ParseDataSubFolderNextRound();
		}

		return;
	}
	async function HandleCSVFile(DataSetName) {
		var CSVRawData = await fs.readFileAsync('data/'+schema.name+'/'+DataSetName+".csv",'utf8');
		var CSVObj =  await csvparse(CSVRawData);
		var HeaderRow = CSVObj.splice(0,1)[0];
		logger.debug(" Got CSV File",schema.name+'.'+DataSetName);
		logger.debug("  Header Row: "+JSON.stringify(HeaderRow),schema.name+'.'+DataSetName);
		
		//ADD CONST COLUMNS
		var ConstColumns = [];		//Need to add constants first, b/c they may also be links
		for (let i in FileSchemas[DataSetName].columns) { if (FileSchemas[DataSetName].columns[i].constant !== undefined) ConstColumns.push(i);}
		if (ConstColumns.length >0 ) {
			logger.debug("  Adding in "+ConstColumns.length+" const cols",schema.name+'.'+DataSetName);
			for (let i=0;i<ConstColumns.length;i++) {
				HeaderRow.push(ConstColumns[i]);
				for (let j=0;j<CSVObj.length;j++) {
					CSVObj[j].push(FileSchemas[DataSetName].columns[FileSchemas[DataSetName].columns[ConstColumns[i]].servesas].constant);
				}
			}
		}

		var ExternalLinkColumns = HeaderRow.filter(FilterExternalLinkCols);
		var InternalLinkColumns = HeaderRow.filter(FilterInternalLinkCols);
		var AKAColumns = HeaderRow.filter(FilterAKACols);
		var MetaColumns = HeaderRow.filter(FilterMetaCols);
		var NormalColumns = HeaderRow.filter(FilterNormalCols);
		var DateColumns = HeaderRow.filter(FilterDateCols);
		
		if (ExternalLinkColumns.length>0) logger.debug("  External Link Columns: "+JSON.stringify(ExternalLinkColumns),schema.name+'.'+DataSetName); 
		if (InternalLinkColumns.length>0) logger.debug("  Internal Link Columns: "+JSON.stringify(InternalLinkColumns),schema.name+'.'+DataSetName); 
		if (AKAColumns.length>0) logger.debug("  AKA Columns: "+JSON.stringify(AKAColumns),schema.name+'.'+DataSetName);
		if (MetaColumns.length>0) logger.debug("  Meta Columns: "+JSON.stringify(MetaColumns),schema.name+'.'+DataSetName);
		if (NormalColumns.length>0) logger.debug("  Normal Columns: "+JSON.stringify(NormalColumns),schema.name+'.'+DataSetName);
		if (DateColumns.length>0) logger.debug("  Date Columns: "+JSON.stringify(DateColumns),schema.name+'.'+DataSetName);
		if (ConstColumns.length>0) logger.debug("  Const Columns: "+JSON.stringify(ConstColumns),schema.name+'.'+DataSetName);
		if (ExternalLinkColumns.length >0 || InternalLinkColumns.length > 0) {
			let LinkIDs = await GetLinkIDsFromDB();
			/*var LinksLookup = new Map();
			for (let i in ExternalLinkColumns) LinksLookup.set(ExternalLinkColumns[i],LinkIDs[i].rows);
			for (let i in InternalLinkColumns) LinksLookup.set(InternalLinkColumns[i],LinkIDs[i].rows);*/
			var LinksLookup = {};
			let AllLinkColumns = ExternalLinkColumns.concat(InternalLinkColumns);
			for (let i in AllLinkColumns) {
				LinksLookup[AllLinkColumns[i]] = new Map();
				for (let j of LinkIDs[i].rows) {
					LinksLookup[AllLinkColumns[i]].set(j.name,j.id);
				}
			}
			//logger.debug("  LinksLookup Map: "+JSON.stringify(Object.entries(LinksLookup).map(function(el) {return [el[0],Array.from(el[1].entries())];})),schema.name+'.'+DataSetName);
		}
/*CURRENT STATUS: 
	First
	Then
		Need to process namespaces into WHERE clauses for external and internal links
		COPY meta data cols
		Deal with AKAs
			[make sure there is a mechanism for a AKA-only CSV]
				Maybe change AKA tables' id seq name to be the same as regular tables, and then could use normal import mechanism from schema_aka folder?
		Resolve columns that have a mapping eg in usa_county (maybe create a map for each column similar to servesas that exists for each column)
		Error Handling
			Do we need TRANSACTION?
			External/Internal links that can't be resolved
		Testing
			JSON syntax makes it seem like can link to field other than name --- does this actually work?
	Then Then
		Will ETL functionality be needed to:
			Read xls/xlsx
			normalize wide data
		Cleanup!

*/
		

		// REWRITE DATE FORMATS
		if (DateColumns.length >0) {
			logger.debug("  Reformatting date values",schema.name+'.'+DataSetName);
			for (let i=0;i<DateColumns.length;i++) {
				let ColPos = HeaderRow.indexOf(DateColumns[i]);
				for (let j=0;j<CSVObj.length;j++) {
					let TempDate = Date.parse(CSVObj[j][ColPos]);
					if (isNaN(TempDate)) {
						CSVObj[j][ColPos] = null;	
					}
					else {
						TempDate = new Date(TempDate);
						CSVObj[j][ColPos] = TempDate.getFullYear()+'-'+(TempDate.getMonth()+1)+'-'+TempDate.getDate();
					}
				}
			}

		}

		// CREATE MAIN IMPORT ARRAY AND ADD NORMAL AND EXTERNAL LINK COLS
		var CopyArray = [];
		var CopyArrayHeader = ["id"].concat(NormalColumns,InternalLinkColumns,ExternalLinkColumns); // Need to rename link columns from eg locale to localeid
		CopyArray.push(CopyArrayHeader);
		for (let i=0;i<CSVObj.length;i++) {									
			var CurRow = Array(CopyArrayHeader.length).fill(null);
			for (let j=0;j<HeaderRow.length;j++) {
				if (ExternalLinkColumns.indexOf(HeaderRow[j]) == -1) {				//Normal data
					CurRow[CopyArrayHeader.indexOf(HeaderRow[j])] = CSVObj[i][j];
				}
				else {																//External Link
					try {
						//CurRow[CopyArrayHeader.indexOf(HeaderRow[j])] = LinksLookup.get(HeaderRow[j]).filter(function(el) {return el.name ==CSVObj[i][j];})[0].id;
						CurRow[CopyArrayHeader.indexOf(HeaderRow[j])] = LinksLookup[HeaderRow[j]].get(CSVObj[i][j]);
					}
					catch (e){
						logger.error("  There was no value in DB named "+CSVObj[i][j]+" for linked column "+HeaderRow[j],schema.name+'.'+DataSetName);
					}
				}

			}
			CopyArray.push(CurRow);
		}

		// RENAME HEADER COLUMNS TO SERVESAS NAMES
		CopyArray[0] = CopyArray[0].map(function (el) {
			if (el=="id") return el;
			else {
				return FileSchemas[DataSetName].columns[el].servesas;
			}
		});

		//RENAME EXTERNAL LINKS IN HEADER
		CopyArray[0] = CopyArray[0].map(function(el) {
			if (ExternalLinkColumns.indexOf(el) != -1) {
				return el+"id";
			}
			else {
				return el;
			}
			
		});

		// SET IDs FOR EACH ROW
		let IDList = await GetTableIDs(schema.name,CopyArray.length-1); 
		logger.verbose("  Pass 1: Retrieved "+IDList.rows.length+" new table IDs",schema.name+'.'+DataSetName);
		for (let i=0;i<CopyArray.length-1;i++) {
			CopyArray[i+1][CopyArrayHeader.indexOf("id")] = IDList.rows[i].nextval;
		}

	

		// RESOLVE INTERNAL LINKS
		//  Bug fix attempted: This currently only resolves internal links within the same file.  Need to read existing data from DB and combine w/data in file
		if (InternalLinkColumns.length>0) {
			logger.verbose("  Pass 2: Resolve internal links: "+InternalLinkColumns/*.map(GetRealColName)*/.join(','),schema.name+'.'+DataSetName);
			for (let i=0;i<InternalLinkColumns.length;i++) {
				let CopyArrayColPos = CopyArrayHeader.indexOf(InternalLinkColumns[i]);
				if (LinksLookup[InternalLinkColumns[i]] === undefined) LinksLookup[InternalLinkColumns[i]] = new Map();
				for (let j=1;j<CopyArray.length;j++) {	//Create map to lookup ID #s
					LinksLookup[InternalLinkColumns[i]].set(CopyArray[j][CopyArrayHeader.indexOf("name")],CopyArray[j][CopyArrayHeader.indexOf("id")]);
				}
				for (let j=1;j<CopyArray.length;j++) {
					CopyArray[j][CopyArrayColPos] = LinksLookup[InternalLinkColumns[i]].get(CopyArray[j][CopyArrayColPos]);
				}
				/*let IDLookup = new Map();
				for (let j=1;j<CopyArray.length;j++) {	//Create map to lookup ID #s
					IDLookup.set(CopyArray[j][CopyArrayHeader.indexOf("name")],CopyArray[j][CopyArrayHeader.indexOf("id")]);
				}
				for (let j=1;j<CopyArray.length;j++) {
					CopyArray[j][CopyArrayColPos] = IDLookup.get(CopyArray[j][CopyArrayColPos]);
				}*/
				CopyArray[0][CopyArrayColPos] += "id";			//Rename header row
				//logger.debug("  New LinksLookup Map: "+JSON.stringify(Object.entries(LinksLookup).map(function(el) {return [el[0],Array.from(el[1].entries())];})),schema.name+'.'+DataSetName);
			}
		}



		

		if (MetaColumns.length>0) {
			logger.debug("  PLACEHOLDER: Create data array to meta data",schema.name+'.'+DataSetName);
		}


		//COPY AKA COLS TO DB
		if (AKAColumns.length >0) {
			var AKAArray = [[schema.name+"id","val"]];
			logger.verbose("  Pass 4: Import AKAs: "+AKAColumns.join(','),schema.name+'.'+DataSetName);
			for (let ColName of AKAColumns) {
				let ColPos = HeaderRow.indexOf(ColName);
				for (let j = 0;j<CSVObj.length;j++) {
					AKAArray.push([CopyArray[j+1][0],CSVObj[j][ColPos]]);
				}
			}
			//logger.debug("AKA Array: "+JSON.stringify(AKAArray),schema.name+'.'+DataSetName);
			let AKAStringData = await csvstringify(AKAArray);
			let s = new stream.Readable();
			s.push(AKAStringData);
			s.push(null);
			try {
				await StreamToDB(schema.name+"_aka",s,AKAArray[0].join(','));
			}
			catch (e) {
				logger.error("StreamToDB Failed",schema.name+'.'+DataSetName);
			}
		}

		//COPY MAIN DATA TO DB
		let CSVStringData = await csvstringify(CopyArray);
		var s = new stream.Readable();
		//s._read = function noop() {};
		s.push(CSVStringData);
		s.push(null);
		try {
			await StreamToDB(schema.name,s,CopyArray[0].join(','));
		}
		catch (e) {
			logger.error("StreamToDB Failed",schema.name+'.'+DataSetName);
		}

		/*
			Resolve external links
				Calculate WHERE clause from namespace
				Generate unique set  of link vals? 
				Build 2-column query (value,ID)
				Run to generate LinksLookup Map
			Prepare data for COPY: Create array of arrays
				Generate list of ID numbers using SELECT nextval('seq') from generate_series(1,DataSetRows.length);
				BEGIN TRANSACTION?
				Push ID numbers into array
				Go through all non-AKA columns & non-internal link cols in schema 
					If in parent schema (i.e. have dedicated col in table), push into main array
						For links: lookup link value in previous query result ---> Error if not found [error only on pass2?]
						For remaining columns: use value with optional mapping
					If not in parent schema (i.e. go in metadata table), push into meta arrat based on data type
						For links: lookup link value in previous query result ---> Error if not found
						For remaining columns: use value with optional mapping
					[Maybe it's possible to add to link table here and then do 2nd pass to resolve internal links]
					[Maybe 2nd pass to resolve links?]
			COPY to main table
			COPY to meta table
			Resolve and COPY AKAs
			END TRANSACTION



		*/


		CSVClosedSet.push(DataSetName);
		CSVOpenSet.splice(CSVOpenSet.indexOf(DataSetName),1);
		return CSVObj;

		async function StreamToDB(Schema,s,cols) {
			var client = await db.connect();
			return new Promise( function(resolve,reject) {
				var stream = client.query(pgCopyFrom('COPY '+Schema+' ('+cols+') FROM STDIN WITH (FORMAT csv, HEADER TRUE);'));
				stream.on('error', function(e) {client.end();logger.error(e,Schema+'.'+DataSetName);reject("Error copying to schema "+Schema);});
				stream.on('end', function() {client.release();resolve(s);});
				s.pipe(stream);
			});
		}
		function GetRealColName(colname) {
			//Returns the servesas colname for given CSV column name --- in other words the schema column that the CSV column is an alias for
			return FileSchemas[DataSetName].columns[colname].servesas;
		}
		function FilterInternalLinkCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) {
				logger.warn("   Column "+colname+" is in CSV file, but not in schema, ignoring",schema.name+'.'+DataSetName);
				return false;
			}
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "link"
				&& FileSchemas[DataSetName].columns[GetRealColName(colname)].link.split('.')[0] == schema.name;
		}
		function FilterExternalLinkCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "link"
				&& FileSchemas[DataSetName].columns[GetRealColName(colname)].link.split('.')[0] != schema.name;
		}
		function FilterAKACols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "aka";
		}
		function FilterDateCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "date";
		}
		function FilterMetaCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			if (FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "aka") return false;
			
			return schema.columns[GetRealColName(colname)] === undefined;
		}
		function FilterNormalCols(colname) {
			return MetaColumns.indexOf(colname) == -1
				&& AKAColumns.indexOf(colname) == -1
				&& ExternalLinkColumns.indexOf(colname) == -1
				&& InternalLinkColumns.indexOf(colname) == -1;
		}

		async function GetLinkIDsFromDB() {
			//Looks up IDs for all internal and external links
			var PromiseSet = [];
			for (let colname of ExternalLinkColumns.concat(InternalLinkColumns)) {
				let ColPos = HeaderRow.indexOf(colname);
				let UniqueList = new Set();
				for (let CSVRow of CSVObj) {
					UniqueList.add(CSVRow[ColPos]);
				}
				//logger.debug("Unique Set of "+colname+": "+JSON.stringify(Array.from(UniqueList)),schema.name+'.'+DataSetName);
				
				//var ForeignColName = FileSchemas[DataSetName].columns[colname].link.split(".").reverse()[0];
				//var ForeignTableName = FileSchemas[DataSetName].columns[colname].link.split(".").reverse()[1];
				var ForeignColName = FileSchemas[DataSetName].columns[FileSchemas[DataSetName].columns[colname].servesas].link.split(".").reverse()[0];
				var ForeignTableName = FileSchemas[DataSetName].columns[FileSchemas[DataSetName].columns[colname].servesas].link.split(".").reverse()[1];
				var DBString = 'SELECT ID,'+ForeignColName+' FROM '+ForeignTableName+' WHERE '+ForeignColName+" IN ('"+Array.from(UniqueList).join("','")+"')";
				DBString += " UNION ALL SELECT orig.ID,aka.VAL FROM "+ForeignTableName+"_aka aka JOIN "+ForeignTableName+" orig ON aka."+ForeignTableName+"ID = orig.id";
				DBString += " WHERE val IN ('"+Array.from(UniqueList).join("','")+"');";
				//logger.debug(" "+DBString,schema.name+'.'+DataSetName);
				PromiseSet.push(db.query(DBString));
			}
			return Promise.all(PromiseSet);
		}
	}
	async function GetTableIDs(SchemaName,Count) {
		let Query = "SELECT nextval('"+SchemaName+"id') FROM generate_series(1, "+Count+");";
		return db.query(Query);


	}

}

