# Andrew Erlichson
# 10gen
# script to start a sharded environment on localhost
# clean everything up
echo "killing mongod and mongos"
killall mongod
killall monogs
echo "removing data files"
rm -rf /data/config
rm -rf /data/shard*
# start a replica set and tell it that it will be a shord0
mkdir -p /data/shard0/rs0 /data/shard0/rs1 /data/shard0/rs2 /data/timedb

# launch the time server
mongod  --logpath "timedb.log" --dbpath /data/timedb --port 27021 --fork  --smallfiles

# launch replSet s0
mongod --replSet s0 --logpath "s0-r0.log" --dbpath /data/shard0/rs0 --port 27017 --fork --shardsvr --smallfiles
mongod --replSet s0 --logpath "s0-r1.log" --dbpath /data/shard0/rs1 --port 27018 --fork --shardsvr --smallfiles
mongod --replSet s0 --logpath "s0-r2.log" --dbpath /data/shard0/rs2 --port 27019 --fork --shardsvr --smallfiles
sleep 5

# connect to one server and initiate the set
mongo --port 27017 << 'EOF'
config = { _id: "s0", members:[
{ _id : 0, host : "localhost:27017" },
{ _id : 1, host : "localhost:27018" },
{ _id : 2, host : "localhost:27019" }]};
rs.initiate(config)
EOF
