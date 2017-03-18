let LogFormats = [
	"ERROR  ",
	"WARN   ",
	"verbose",
	"debug  ",
	"silly  ",
	"-ToDo- "
];
let LogLevels = [
	"error",
	"warn",
	"verbose",
	"debug",
	"silly",
	"todo"
];
let MaxMeta = 36;
exports.level = 'warn';

for (let i of LogLevels) {
	exports[i] = function(iv) {
		return function(msg,meta) {
			if (meta === undefined) meta = "";
			let PaddedMeta = meta.length < MaxMeta ? meta + ' '.repeat(MaxMeta - meta.length) : meta.substr(0,MaxMeta/2-1)+'...'+meta.substr(-MaxMeta/2+2);
			if (LogLevels.indexOf(iv) <= LogLevels.indexOf(exports.level)) {
				console.log(LogFormats[LogLevels.indexOf(iv)]+" ["+PaddedMeta+"] - "+msg);
			}
			
		};
	}(i);
}