var Promise = require('bluebird');
const stream = require('stream');
var fs = Promise.promisifyAll(require('fs'));
var csvparse = Promise.promisify(require('csv-parse'));
var csvstringify = Promise.promisify(require('csv-stringify'));
var merge = require('deepmerge');
var XLSX = require('xlsx');
var logger = require('./util/logger');
var file = require('./util/file');
var secrets = require("./secrets");
logger.level = "debug";

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
		await file.ClearResultDir();
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
	
	var ParseRound = 0;  var PrevOpenLen = -1;
	await CreateDatabaseTables(DataFileObj);
	let AKAEntries = [];
	for (let schema of DataFileObj) {
		if (schema.type == "dim") {
			//Auto create aka table's schema in DataFileObj, so that files in that folder can be parsed if needed
			let AKAEntry = {};
			AKAEntry.name = schema.name+'_aka';
			AKAEntry.type = schema.type;
			AKAEntry.dependencies = [schema.name];
			AKAEntry.columns = {val:{type:"string"}};
			AKAEntry.columns[schema.name] =  {type:"link",link:schema.name+".name"};
			AKAEntries.push(AKAEntry);
		}
	}
	DataFileObj = DataFileObj.concat(AKAEntries);

	DataFileObj.forEach(function(el) {OpenSet.push(el.name);});				//add all data sets to Open Set
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
			PromiseSet.push(db.query("CREATE SEQUENCE "+schema.name+"_metaid START 1; "));
			if (schema.type =="dim") {
				PromiseSet.push(db.query("CREATE SEQUENCE "+schema.name+"_akaid START 1; "));
			}
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
			let CreateStrMeta = "CREATE TABLE "+schema.name+"_meta (id integer PRIMARY KEY DEFAULT nextval('"+schema.name+"_metaid'), "+schema.name+"id int, type varchar(1000), val varchar(1000));";
			if (schema.type=="dim") {
				let CreateStrAka = "CREATE TABLE "+schema.name+"_aka (id integer PRIMARY KEY DEFAULT nextval('"+schema.name+"_akaid'), "+schema.name+"id int, val varchar(1000));";
				PromiseSet.push(db.query(CreateStrAka));
			}
			PromiseSet.push(db.query(CreateStrMeta));
			

		}	
		return Promise.all(PromiseSet);
	}
}

function Cont() {
	logger.silly("Continuing...");

}

function GenericAbortError(e) {
	logger.error(e);
	logger.error(e.stack);
	process.exitCode = 1;
}

async function LoadDataSetAsync(schema) {
	/*
		Load file names
		Add all .csv filenames to CSVOpenSet
		Read all files
			if there is an all.json, read it and add values to el
			Add all other JSON files's data to individualized copies of el
		THEN
		ParseDataSubFolderNextRound()



	*/

	//var JSONOpenSet = [];
	var CSVOpenSet = [];
	var CSVClosedSet = [];
	var CSVAKASet = new Set();
	var FileSchemas = {};
	var DataSubFolderRound = 0;
	var PrevOpenLen = -1;
	logger.debug("  Loading data for schema",schema.name);

	//logger.debug("Initial schema is:");
	//logger.debug(JSON.stringify(schema,null,4));
	try {
		var FileNameList = await fs.readdirAsync('data/'+schema.name);
	}
	catch (e) {
		return;
	}
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
			else if (fn.slice(-8).toLowerCase() == '_aka.csv') {
				logger.debug("   "+fn,schema.name);
				//CSVAKASet.add(fn.slice(0,-4));
				CSVAKASet.add(fn);
			}
			else if (fn.slice(-4).toLowerCase() == '.csv' || fn.slice(-4).toLowerCase() == '.xls' || fn.slice(-5).toLowerCase() == '.xlsx') {
				logger.debug("   "+fn,schema.name);
				//CSVOpenSet.push(fn.slice(0,-4));
				CSVOpenSet.push(fn);
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
			logger.silly("Extending default "+schema.name+" schema.  New schema: "+JSON.stringify(schema,null,4));
		}
		else if (DefaultSchemaData.length >1) throw("    Found more than one default schema JSON file");
		for (let DataObj of JSONData.filter(function(el) {return el.name.toLowerCase() != "all" && el.name.toLowerCase() != schema.name.toLowerCase();})) {
			if (DataObj.name === undefined)  throw("    No name in JSON file - "+JSON.stringify(DataObj));
			FileSchemas[DataObj.name] = merge(schema,DataObj,{clone:true});
			if (DataObj.columns !== undefined) FileSchemas[DataObj.name].columns = merge(schema.columns,DataObj.columns,{clone:true});
			for (let col in FileSchemas[DataObj.name].columns) { 
				//Add servesas to all columns that don't have them (so we can easily find the logical col name later)
				if (FileSchemas[DataObj.name].columns[col].servesas === undefined) FileSchemas[DataObj.name].columns[col].servesas = col;
			}
			logger.silly("Read individual file schema for "+schema.name+"/"+DataObj.name+".  Schema is:"+JSON.stringify(FileSchemas[DataObj.name],null,4));
		}
		for (let CSVNameWExt of CSVOpenSet) {
			let CSVName = file.RemoveFileExtension(CSVNameWExt);
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
		for (let CurDataSetNameWExt of CSVOpenSet) {
			let CurDataSetName = file.RemoveFileExtension(CurDataSetNameWExt);
			let CurSchema = FileSchemas[CurDataSetName];
			logger.debug(" Dependencies: "+JSON.stringify(CurSchema.dependencies),schema.name+'.'+CurDataSetName);
			if (CurSchema.dependencies === undefined || CurSchema.dependencies.filter(function(d) { 
				return d.startsWith(schema.name+'.') && CSVClosedSet.indexOf(d.slice(schema.name.length+1)) == -1;
			}).length == 0) {
				logger.verbose(" Parsing ",schema.name+"."+CurDataSetName);
				PromiseSet.push(HandleCSVFile(CurDataSetNameWExt));	
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
	async function HandleCSVFile(DataSetNameWExt) {
		var CSVRawData,CSVObj;
		if (DataSetNameWExt.slice(-4) == '.csv') {
			CSVRawData = await fs.readFileAsync('data/'+schema.name+'/'+DataSetNameWExt,'utf8');
			CSVObj =  await csvparse(CSVRawData);
		}
		if (DataSetNameWExt.slice(-4) == '.xls' || DataSetNameWExt.slice(-5) == '.xlsx') {
			CSVObj = await GetXLSData(DataSetNameWExt);
		}
		var DataSetName = file.RemoveFileExtension(DataSetNameWExt);
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

		//ADD VIRTUAL COLUMNS
		var	VirtualColumns = [];
		for (let i in FileSchemas[DataSetName].columns) { if (FileSchemas[DataSetName].columns[i].virtualcalc !== undefined) VirtualColumns.push(i);}
		if (VirtualColumns.length >0) {
			logger.debug("  Adding in "+VirtualColumns.length+" virtual cols",schema.name+'.'+DataSetName);
			for (let i=0;i<VirtualColumns.length;i++) {
				HeaderRow.push(VirtualColumns[i]);
				for (let j=0;j<CSVObj.length;j++) {
					CSVObj[j].push(GetVirtualCalcVal(CSVObj[j],FileSchemas[DataSetName].columns[VirtualColumns[i]].virtualcalc));
				}
			}
		}

		var ExternalLinkColumns = HeaderRow.filter(FilterExternalLinkCols);
		var InternalLinkColumns = HeaderRow.filter(FilterInternalLinkCols);
		var AKAColumns = HeaderRow.filter(FilterAKACols);
		var MetaColumns = HeaderRow.filter(FilterMetaCols);
		var NormalColumns = HeaderRow.filter(FilterNormalCols);
		var DateColumns = HeaderRow.filter(FilterDateCols);

		var ExternalAKAMap = new Map();
		var HasExternalAKA = CSVAKASet.has(DataSetName+'_aka.csv');
		
		if (ExternalLinkColumns.length>0) logger.debug("  External Link Columns: "+JSON.stringify(ExternalLinkColumns),schema.name+'.'+DataSetName); 
		if (InternalLinkColumns.length>0) logger.debug("  Internal Link Columns: "+JSON.stringify(InternalLinkColumns),schema.name+'.'+DataSetName); 
		if (AKAColumns.length>0) logger.debug("  AKA Columns: "+JSON.stringify(AKAColumns),schema.name+'.'+DataSetName);
		if (MetaColumns.length>0) logger.debug("  Meta Columns: "+JSON.stringify(MetaColumns),schema.name+'.'+DataSetName);
		if (NormalColumns.length>0) logger.debug("  Normal Columns: "+JSON.stringify(NormalColumns),schema.name+'.'+DataSetName);
		if (DateColumns.length>0) logger.debug("  Date Columns: "+JSON.stringify(DateColumns),schema.name+'.'+DataSetName);
		if (ConstColumns.length>0) logger.debug("  Const Columns: "+JSON.stringify(ConstColumns),schema.name+'.'+DataSetName);
		if (ExternalLinkColumns.length >0 || InternalLinkColumns.length > 0) {
			let LinkIDs = await GetLinkIDsFromDB();
			var LinksLookup = {};
			let AllLinkColumns = ExternalLinkColumns.concat(InternalLinkColumns);
			for (let i in AllLinkColumns) {
				LinksLookup[AllLinkColumns[i]] = new Map();
				for (let j of LinkIDs[i].rows) {
					LinksLookup[AllLinkColumns[i]].set(j.name,j.id);
				}
			}
			logger.silly("  LinksLookup Map: "+JSON.stringify(Object.entries(LinksLookup).map(function(el) {return [el[0],Array.from(el[1].entries())];})),schema.name+'.'+DataSetName);
		}
/*CURRENT STATUS: 
	First
		Subfolders
			Perhaps: https://github.com/fshost/node-dir
			or: https://github.com/pvorb/node-dive
			or: https://github.com/thlorenz/readdirp
		Flag general vs primary?
	Then
		Create README and LICENSE files
		Need to process namespaces into WHERE clauses for external and internal links
		Read xls/xls -- started with GetXLSData
			Added, but still need to add reading _aka files from xls/xlsx
		Error Handling
			Do we need TRANSACTION?
			External/Internal links that can't be resolved. e.g. - Currently const usa_states parent can't be resolved
			Dependencies on data sources that don't exist (currently not flagged, probably should be WARN or ERROR)
		Testing
			JSON syntax makes it seem like can link to field other than name --- does this actually work?
	Then Then
		Command line options
		Will ETL functionality be needed to:
			normalize wide data
		Add license text into DB so that it can ultimately be displayed on website
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
		var CopyArrayHeader = ["id"].concat(NormalColumns,InternalLinkColumns,ExternalLinkColumns); 
		CopyArray.push(CopyArrayHeader);
		let UnmatchedLinks = {};
		for (let i=0;i<CSVObj.length;i++) {									
			var CurRow = Array(CopyArrayHeader.length).fill(null);
			for (let j=0;j<HeaderRow.length;j++) {
				if (CopyArrayHeader.indexOf(HeaderRow[j]) == -1) continue;
				if (ExternalLinkColumns.indexOf(HeaderRow[j]) == -1) {				//Normal data
					CurRow[CopyArrayHeader.indexOf(HeaderRow[j])] = TransformVal(CSVObj[i][j],HeaderRow[j]);
				}
				else {																//External Link
					if (LinksLookup[HeaderRow[j]]===undefined) {
						logger.error("  There was no Lookup Map for external link col "+HeaderRow[j],schema.name+'.'+DataSetName);
					}
					else if (LinksLookup[HeaderRow[j]].get(TransformVal(CSVObj[i][j],HeaderRow[j]))=== undefined && CSVObj[i][j] != "") {
						logger.silly("HeaderRow[j]: "+HeaderRow[j],schema.name+'.'+DataSetName);
						logger.silly("FileSchemas[DataSetName].columns[HeaderRow[j]]: "+JSON.stringify(FileSchemas[DataSetName].columns[HeaderRow[j]]),schema.name+'.'+DataSetName);
						let LinkedTableName = FileSchemas[DataSetName].columns[GetRealColName(HeaderRow[j])].link.split('.')[0];
						if (UnmatchedLinks[LinkedTableName] === undefined) UnmatchedLinks[LinkedTableName] = new Set();
						UnmatchedLinks[LinkedTableName].add(TransformVal(CSVObj[i][j],HeaderRow[j]));
					}
					if (LinksLookup[HeaderRow[j]] !== undefined) {
						CurRow[CopyArrayHeader.indexOf(HeaderRow[j])] = LinksLookup[HeaderRow[j]].get(TransformVal(CSVObj[i][j],HeaderRow[j]));
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
			if (ExternalLinkColumns.map(GetRealColName).indexOf(el) != -1) {
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
			if (HasExternalAKA) {
				ExternalAKAMap.set(CopyArray[i+1][CopyArrayHeader.indexOf("name")],IDList.rows[i].nextval);
			}
		}

	

		// RESOLVE INTERNAL LINKS
		if (InternalLinkColumns.length>0) {
			logger.verbose("  Pass 2: Resolve internal links: "+InternalLinkColumns/*.map(GetRealColName)*/.join(','),schema.name+'.'+DataSetName);
			for (let i=0;i<InternalLinkColumns.length;i++) {
				let CopyArrayColPos = CopyArrayHeader.indexOf(InternalLinkColumns[i]);
				if (LinksLookup[InternalLinkColumns[i]] === undefined) LinksLookup[InternalLinkColumns[i]] = new Map();
				for (let j=1;j<CopyArray.length;j++) {	//Create map to lookup ID #s
					LinksLookup[InternalLinkColumns[i]].set(CopyArray[j][CopyArrayHeader.indexOf("name")],CopyArray[j][CopyArrayHeader.indexOf("id")]);
				}
				for (let j=1;j<CopyArray.length;j++) {
					if (LinksLookup[InternalLinkColumns[i]] === undefined) {
						logger.error("  There was no Lookup Map for internal link col "+InternalLinkColumns[i],schema.name+'.'+DataSetName);
					}
					
					else if (LinksLookup[InternalLinkColumns[i]].get(CopyArray[j][CopyArrayColPos]) === undefined && CopyArray[j][CopyArrayColPos] != '') {
						let LinkedTableName = FileSchemas[DataSetName].columns[InternalLinkColumns[i]].link.split('.')[0];
						if (UnmatchedLinks[LinkedTableName] === undefined) UnmatchedLinks[LinkedTableName] = new Set();
						UnmatchedLinks[LinkedTableName].add(CopyArray[j][CopyArrayColPos]);
					}
					if (LinksLookup[InternalLinkColumns[i]] !== undefined) {
						CopyArray[j][CopyArrayColPos] = LinksLookup[InternalLinkColumns[i]].get(CopyArray[j][CopyArrayColPos]);
					}
					
				}
				CopyArray[0][CopyArrayColPos] += "id";			//Rename header row
				logger.silly("  New LinksLookup Map: "+JSON.stringify(Object.entries(LinksLookup).map(function(el) {return [el[0],Array.from(el[1].entries())];})),schema.name+'.'+DataSetName);
			}
		}



		//Copy META COLS TO DB
		if (MetaColumns.length >0 ) {
			let MetaArray = [[schema.name+'id','type','val']];
			logger.verbose("  Pass 3: Import Meta Cols: "+MetaColumns.join(','),schema.name+'.'+DataSetName);
			for (let ColName of MetaColumns) {
				let ColPos = HeaderRow.indexOf(ColName);
				for (let j = 0;j<CSVObj.length;j++) {
					MetaArray.push([CopyArray[j+1][0],GetRealColName(ColName),TransformVal(CSVObj[j][ColPos],ColName)]);
				}
			}
			logger.silly("Meta Array: "+JSON.stringify(MetaArray),schema.name+'.'+DataSetName);
			await CopyArrayToDB(schema.name+'_meta',MetaArray);
		}

		//COPY AKA COLS TO DB
		if (AKAColumns.length >0) {
			let AKAArray = [[schema.name+"id","val"]];
			logger.verbose("  Pass 4: Import Internal AKAs: "+AKAColumns.join(','),schema.name+'.'+DataSetName);
			for (let ColName of AKAColumns) {
				let ColPos = HeaderRow.indexOf(ColName);
				for (let j = 0;j<CSVObj.length;j++) {
					AKAArray.push([CopyArray[j+1][0],TransformVal(CSVObj[j][ColPos],ColName)]);
				}
			}
			logger.silly("AKA Array: "+JSON.stringify(AKAArray),schema.name+'.'+DataSetName);
			await CopyArrayToDB(schema.name+'_aka',AKAArray);
		}

		//WARN UNMATCHED LINKS
		for (let col in UnmatchedLinks) {
			logger.warn("  Unmatched link(s) for: "+col+". See results/"+col+"_aka.csv",schema.name+'.'+DataSetName);
			let UnmatchedString = "name,val\n";
			for (let val of Array.from(UnmatchedLinks[col].values())) {
				UnmatchedString += ','+val+"\n";
			}
			file.SaveCSVFile("results",col+"_aka.csv",UnmatchedString);
		}


		//COPY MAIN DATA TO DB
		await CopyArrayToDB(schema.name,CopyArray);

		//IMPORT AKAs FROM EXTERNAL AKA FILE
		if (HasExternalAKA) {
			logger.verbose("  Pass 5: Import AKAs from external file: "+DataSetName+'_aka.csv',schema.name+'.'+DataSetName);
			var AKACSVRawData = await fs.readFileAsync('data/'+schema.name+'/'+DataSetName+"_aka.csv",'utf8');
			var AKACSVObj =  await csvparse(AKACSVRawData);
			//var AKAHeaderRow = AKACSVObj.splice(0,1)[0];
			let NamePos = AKACSVObj[0].indexOf("name");
			for (let i =1;i<AKACSVObj.length;i++) {
				AKACSVObj[i][NamePos] = ExternalAKAMap.get(AKACSVObj[i][NamePos]);
			}
			AKACSVObj[0][NamePos] = schema.name+'id';
			await CopyArrayToDB(schema.name+'_aka',AKACSVObj);

		}



		CSVClosedSet.push(DataSetName);
		CSVOpenSet.splice(CSVOpenSet.indexOf(DataSetNameWExt),1);
		return CSVObj;

		async function CopyArrayToDB(tablename,array) {
			let CSVStringData = await csvstringify(array);
			var s = new stream.Readable();
			s.push(CSVStringData);
			s.push(null);
			try {
				await StreamToDB(tablename,s,array[0].join(','));
			}
			catch (e) {
				logger.error("StreamToDB Failed",schema.name+'.'+DataSetName);
			}

		}
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
		function TransformVal(val,colname) {
			//Returns the given value with any transformations specified by the column schema applied
			//val = (val === null || val === undefined) ? null : val.replace(/'/g, "''").trim();
			if (val === null || val === undefined) return val;
			val = val.replace(/'/g, "''").trim();
			if (FileSchemas[DataSetName].columns[colname].mapping !== undefined) {
				let match = undefined;
				if (FileSchemas[DataSetName].columns[colname].mapping.regex !== undefined) {
					for (let restr in FileSchemas[DataSetName].columns[colname].mapping.regex) {
						let re = new RegExp(restr);
						if (val.search(re) != -1) {
							match = val.replace(re,FileSchemas[DataSetName].columns[colname].mapping.regex[restr]);
						}
					}
				}
				if (FileSchemas[DataSetName].columns[colname].mapping.direct !== undefined) {
					if (FileSchemas[DataSetName].columns[colname].mapping.direct[val] !== undefined) {
						match = FileSchemas[DataSetName].columns[colname].mapping.direct[val];
					}
				}
				if (match  === undefined) {
					logger.warn("Mapping undefined for val: '"+val+"' of column: '"+colname+"'",schema.name+'.'+DataSetName);
				}
				else {
					val = match;
				}
			}
			if (FileSchemas[DataSetName].columns[colname].prefix !== undefined) val = FileSchemas[DataSetName].columns[colname].prefix+val;
			if (FileSchemas[DataSetName].columns[colname].suffix !== undefined) val = FileSchemas[DataSetName].columns[colname].suffix+val;
			return val;
		}
		function GetVirtualCalcVal(row,virtualcalc) {
			var curval;
			for (let step of virtualcalc) {
				if (step[0] == "readfrom") {
					curval = row[HeaderRow.indexOf(step[1])];
				}
				else {
					logger.error("Unknown virtual calculation: "+step[0],schema.name+'.'+DataSetName);
				}
			}
			return curval;
		}
		function FilterInternalLinkCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) {
				logger.warn("   Column "+colname+" is in CSV file, but not in schema, ignoring",schema.name+'.'+DataSetName);
				return false;
			}
			if (FileSchemas[DataSetName].columns[GetRealColName(colname)] === undefined) return false;
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "link"
				&& FileSchemas[DataSetName].columns[GetRealColName(colname)].link.split('.')[0] == schema.name;
		}
		function FilterExternalLinkCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			if (FileSchemas[DataSetName].columns[GetRealColName(colname)] === undefined) return false;
			
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "link"
				&& FileSchemas[DataSetName].columns[GetRealColName(colname)].link.split('.')[0] != schema.name;
		}
		function FilterAKACols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			if (FileSchemas[DataSetName].columns[GetRealColName(colname)] === undefined) return false;
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "aka";
		}
		function FilterDateCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			if (FileSchemas[DataSetName].columns[GetRealColName(colname)] === undefined) return false;
			return FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "date";
		}
		function FilterMetaCols(colname) {
			if (FileSchemas[DataSetName].columns[colname] === undefined) return false;
			if (FileSchemas[DataSetName].columns[GetRealColName(colname)] !== undefined
				&& FileSchemas[DataSetName].columns[GetRealColName(colname)].type == "aka") return false;
			
			return schema.columns[GetRealColName(colname)] === undefined;
		}
		function FilterNormalCols(colname) {
			return MetaColumns.indexOf(colname) == -1
				&& AKAColumns.indexOf(colname) == -1
				&& ExternalLinkColumns.indexOf(colname) == -1
				&& InternalLinkColumns.indexOf(colname) == -1
				&& (FileSchemas[DataSetName].columns[colname] !== undefined);
		}

		async function GetLinkIDsFromDB() {
			//Looks up IDs for all internal and external links
			var PromiseSet = [];
			for (let colname of ExternalLinkColumns.concat(InternalLinkColumns)) {
				let ColPos = HeaderRow.indexOf(colname);
				let UniqueList = new Set();
				for (let CSVRow of CSVObj) {
					UniqueList.add(TransformVal(CSVRow[ColPos],colname));
				}
				logger.silly("Unique Set of "+colname+": "+JSON.stringify(Array.from(UniqueList)),schema.name+'.'+DataSetName);
				
				var ForeignColName = FileSchemas[DataSetName].columns[FileSchemas[DataSetName].columns[colname].servesas].link.split(".").reverse()[0];
				var ForeignTableName = FileSchemas[DataSetName].columns[FileSchemas[DataSetName].columns[colname].servesas].link.split(".").reverse()[1];
				var DBString = 'SELECT ID,'+ForeignColName+' FROM '+ForeignTableName+' WHERE '+ForeignColName+" IN ('"+Array.from(UniqueList).join("','")+"')";
				DBString += " UNION ALL SELECT orig.ID,aka.VAL FROM "+ForeignTableName+"_aka aka JOIN "+ForeignTableName+" orig ON aka."+ForeignTableName+"ID = orig.id";
				DBString += " WHERE val IN ('"+Array.from(UniqueList).join("','")+"');";
				logger.silly(" "+DBString,schema.name+'.'+DataSetName);
				PromiseSet.push(db.query(DBString));
			}
			return Promise.all(PromiseSet);
		}
	}
	async function GetTableIDs(SchemaName,Count) {
		let Query = "SELECT nextval('"+SchemaName+"id') FROM generate_series(1, "+Count+");";
		return db.query(Query);


	}
	async function GetXLSData(filename) {
		var raw = await fs.readFileAsync('data/'+schema.name+'/'+filename,'binary');
		var workbook = XLSX.read(raw, {type:"binary"});
		var first_sheet_name = workbook.SheetNames[0];
		return XLSX.utils.sheet_to_json(workbook.Sheets[first_sheet_name],{header:1});
		
	}

}

