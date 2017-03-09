var Promise = require('bluebird');
var fs = Promise.promisifyAll(require('fs'));

exports.SaveCSVFile = async function (path,filename,str) {
	try {
		let SyncVal =  await fs.statAsync(path);
		if (!SyncVal.isDirectory()) {
			throw("results exists, but is not a directory");
		}
	}
	catch (e) {

		await fs.mkdirAsync(path);
	}

	fs.appendFileAsync(path+'/'+filename,str);

};

exports.ClearResultDir = async function() {
	try {
		let Filenames = await fs.readdirAsync("results");
		for (let fn of Filenames) {
			fs.unlinkAsync("results/"+fn);
		}
	}
	catch (e) {
		//
	}
};