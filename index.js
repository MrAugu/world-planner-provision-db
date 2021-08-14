const dotenv = require("dotenv");
dotenv.config();
const mysql = require("mysql2/promise");
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const SnowflakeId = require("snowflake-id");
const snowflake = new SnowflakeId.default({
  mid: 1,
  offset: (2021 - 1970)* 31536000 * 1000
})

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
  textureFiles.push(`${name}`);
}

textureFiles = [...new Set(textureFiles)];
const missingTextures = [];

for (const texture of textureFiles) {
  const texturePath = path.resolve(process.cwd(), `./textures/${texture}.png`);
  if (!fs.existsSync(texturePath)) missingTextures.push(texture);
  continue;
}
if (missingTextures.length) return console.log(`!! Missing textures. A total of ${missingTextures.length} textures are missing: ${missingTextures.join(", ")}.`);

const textures = [];
for (const texture of textureFiles) {
  const texturePath = path.resolve(process.cwd(), `./textures/${texture}.png`);
  const $texture = {};
  $texture.data = fs.readFileSync(texturePath);
  $texture.hash = crypto.createHash("sha1")
    .update($texture.data)
    .digest("hex");
  $texture.id = snowflake.generate();
  $texture.name = `${texture}.png`;
  textures.push($texture);
}

(async function() {
  const connection = await mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD
  });
  console.log("Connected to the database, running data pruning and replacing sequence.");
  await connection.query('CREATE DATABASE IF NOT EXISTS `world_planner`;');
  await connection.query('USE `world_planner`;');
  await connection.query("DROP TABLE IF EXISTS `items`;");
  await connection.query("DROP TABLE IF EXISTS `textures`;");
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`items\` (
    \`id\` VARCHAR(20),
    \`game_id\` INT SIGNED,
    \`action_type\` SMALLINT SIGNED,
    \`item_category\` SMALLINT SIGNED,
    \`name\` VARCHAR(150),
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
  await connection.query(`
  CREATE TABLE IF NOT EXISTS \`textures\` (
    \`id\` BIGINT,
    \`name\` VARCHAR(50),
    \`hash\` CHAR(40),
    \`contents\` LONGBLOB
  );`);

  console.log("Data removed, tables re-created - inserting textures.");
  const textureBeginTime = Date.now();
  for (const texture of textures) {
    await connection.query("INSERT INTO `textures` (id, name, hash, contents) VALUES (?, ?, ?, ?)", [
      BigInt(texture.id),
      texture.name,
      texture.hash,
      texture.data
    ]);
    console.log(`Inserted texture ${texture.name}.`);
  }
  const textureEndTime = Date.now();
  console.log("Textures have been inserted.");

  const itemsBeginTime = Date.now();
  for (const item of items) {
    if (item.id % 100 === 0) console.log(`Inserting item with id #${item.id}.`);
    const itemIdBuffer = Buffer.alloc(9);
    itemIdBuffer.writeInt16LE(Math.floor(Math.random() * 32767));
    itemIdBuffer.writeInt32BE(item.id, 2);
    itemIdBuffer.writeInt8(Math.floor(Math.random() * 20), 6);
    itemIdBuffer.writeInt16LE(Math.floor(Math.random() * 32767), 7);

    await connection.query(`INSERT INTO \`items\` (id, game_id, action_type, item_category, name, texture,
    texture_hash, texture_x, texture_y, spread_type, collision_type, rarity, max_amount, break_hits)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
      itemIdBuffer.toString("hex"),
      item.id,
      coerceIntoType(item.actionType),
      item.item_category,
      item.name,
      `${item.texture.split(".")[0]}.png`,
      textures.find(texture => texture.name === `${item.texture.split(".")[0]}.png`).hash,
      item.texture_x,
      item.texture_y,
      item.spread_type,
      item.collision_type,
      item.rarity,
      item.max_amount || 1,
      Math.floor(item.break_hits / 6) || 1
    ]);
  }
  const itemsEndTime = Date.now();
  console.log(`Took ${(textureEndTime - textureBeginTime).toLocaleString()}ms for textures to get inserted.`);
  console.log(`Took ${(itemsEndTime - itemsBeginTime).toLocaleString()}ms for items to get inserted.`);
}());

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
