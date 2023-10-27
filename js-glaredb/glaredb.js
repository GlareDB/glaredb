const glaredb = require('./index.js')

Object.assign(glaredb.JsLogicalPlan.prototype, {
  async toPolars() {
    try {
      const pl = require("nodejs-polars")
      let arrow = await this.toIpc();
      return pl.readIPC(arrow)
    } catch (e) {
      throw new Error("polars is not installed, please run `npm install nodejs-polars`")
    }
  },
  async toArrow() {
    try {
      const arrow = require("apache-arrow")
      let buf = await this.toIpc();
      return arrow.tableFromIPC(buf)

    } catch (e) {
      throw new Error("apache-arrow is not installed, please run `npm install apache-arrow`")
    }
  }
});

module.exports = glaredb
