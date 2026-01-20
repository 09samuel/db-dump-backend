function resolveCapabilitiesByEngine(engine) {
  switch (engine.toLowerCase()) {
    case "postgresql":
    case "mysql":
      return {
        modes: ["FULL", "STRUCTURE_ONLY", "DATA_ONLY"],
        formats: ["CUSTOM", "PLAIN"],
        compression: true
      };

    case "mongodb":
      return {
        modes: ["FULL", "DATA_ONLY"],
        formats: ["ARCHIVE"],
        compression: true
      };

    default:
      return {
        modes: ["FULL"],
        formats: [],
        compression: false
      };
  }
}

module.exports = { resolveCapabilitiesByEngine };