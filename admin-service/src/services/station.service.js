const prisma = require("../config/prisma");
const { ConflictError, NotFoundError } = require("../utils/error");
const logger = require('../config/logger');
const adminProducer = require('../kafka/producer/admin.producer');

const createStation = async (data) => {
     const existing = await prisma.station.findUnique({
          where: { code: data.code }
     });

     if (existing) {
          throw new ConflictError('Station code already exists');
     }

     let station;
     try {
          station = await prisma.station.create({
               data
          });
     } catch (err) {
          // `name` is unique as well, and the check above only covers `code`.
          // Without this, a name collision surfaces as a raw Prisma P2002 ->
          // HTTP 500 instead of a 409.
          if (err.code === 'P2002') {
               throw new ConflictError('Station name or code already exists');
          }
          throw err;
     }

     logger.info('Station Created', { id: station.id, code: station.code });

     // Publish event - fire and forget (non-critical)
     await adminProducer.publishStationCreated(station).catch((err) => {
          logger.error('Failed to publish station created event', { error: err.message });
     });
     return station;
}

const getAllStations = async (page, limit, search) => {
     const skip = (page - 1) * limit;

     const where = search ? {
          OR: [
               { code: { contains: search, mode: 'insensitive' } },
               { name: { contains: search, mode: 'insensitive' } },
               { city: { contains: search, mode: 'insensitive' } }
          ]
     } : {};

     const [stations, total] = await Promise.all([
          prisma.station.findMany({
               where,
               skip,
               take: limit,
               orderBy: {
                    name: 'asc'
               }
          }),
          prisma.station.count({ where })
     ]);

     return { stations, total };
};

const getStationById = async (stationId) => {
     const station = await prisma.station.findUnique({
          where: { id: stationId }
     });

     if (!station) {
          throw new NotFoundError('Station not found');
     }

     return station;
};


module.exports = { createStation, getAllStations, getStationById };