const dotenv = require("dotenv");
dotenv.config();
const mysql = require("mysql2/promise");
const path = require("path");
const fs = require("fs");

const inputFilePath = path.resolve(process.cwd(), process.argv[2] || "384390");
if (!fs.existsSync(inputFilePath)) return console.log("You must specify a valid ");
let itemData;

try {
  itemData = fs.readFileSync(inputFilePath, { encoding: "utf8" });
  itemData = JSON.parse(itemData);
  if (!itemData.item_dat_version || !itemData.items || itemData.items.length < 11000) throw new Error("Invalid data file.");
} catch (err) {
  return console.log(`An error occured when parsing the item data:\n${err}`);
}

let items = itemData.items;
let textureFiles = [];
items = items.filter(item => ["FIST", "FOREGROUND", "BACKGROUND"].includes(coerceIntoType(item.action_type)));

for (const item of items) {
  let [name, extension] = item.texture.split(".");
  textureFiles.push(`${name}.png`);
}

textureFiles = [...new Set(textureFiles)];
const missingTextures = [];

for (const texture of textureFiles) {
  const texturePath = path.resolve(__dirname, `/textures/${texture}`);
  if (!fs.existsSync(texturePath)) missingTextures.push(texture);
  continue;
}

if (missingTextures.length) return console.log(`!! Missing textures. A total of ${missingTextures.length} textures are missing: ${missingTextures.join(", ")}.`);

(async function() {
  console.log("[MySQL]: Creating a connection..");
  const connection = await mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD
  });
  console.log("[MySQL]: Connection has been created.");
  console.log("[Database]: Running database creation query..");
  await connection.query('CREATE DATABASE IF NOT EXISTS `world_planner`;');
  await connection.query('USE `world_planner`;');
  console.log("[Database]: Ran the database creation query.");
  console.log("[Database]: Running table dropping query..");
  await connection.query("DROP TABLE IF EXISTS `items`;");
  await connection.query("DROP TABLE IF EXISTS `textures`;");
  console.log("[Database]: Ran table dropping queue.");
  console.log("[Database]: Running item table creation query..");
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`items\` (
    \`id\` BIGINT SIGNED,
    \`game_id\` INT SIGNED,
    \`action_type\` SMALLINT SIGNED,
    \`item_category\` SMALLINT SIGNED,
    \`name\` VARCHAR(50),
    \`texture\` VARCHAR(50),
    \`texture_hash\` CHAR(40),
    \`texture_x\` SMALLINT SIGNED,
    \`texture_y\` SMALLINT SIGNED,
    \`spread_type\` SMALLINT SIGNED,
    \`collision_type\` SMALLINT SIGNED,
    \`rarity\` SMALLINT SIGNED,
    \`max_amount\` SMALLINT SIGNED,
    \`break_hits\` SMALLINT SIGNED
  );
  `);
  console.log(`[Database]: Ran the items table creation query.`);
  console.log(`[Database]: Running textures tables creation query..`);
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`textures\` (
    \`id\` BIGINT,
    \`name\` VARCHAR(50),
    \`hash\` CHAR(40),
    \`contents\` LONGBLOB
  );`);
  console.log(`[Database]: Ran textures table creation query.`);

}())

function coerceIntoType(actionType) {
  if (actionType === 0) return "FIST";
  else if (actionType === 1) return "TOOL";
  else if ([8, 37, 44, 48, 64, 107, 121, 133, 137].includes(actionType)) return "NONE";
  else if ([18, 22, 23, 28].includes(actionType)) return "BACKGROUND";
  else if (actionType === 19) return "SEED";
  else if (actionType === 20) return "CLOTH";
  else if (actionType === 129) return "COMPONENT";
  return "FOREGROUND";
}