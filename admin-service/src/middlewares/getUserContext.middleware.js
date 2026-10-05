const { UnauthorizedError, ForbiddenError } = require('../utils/error');

/**
 * Extract user context from gateway headers.
 *
 * The gateway verifies the JWT and overwrites x-user-id / x-user-role, so a
 * header forged by calling this service directly is replaced before it gets
 * here. This service is only reachable through the gateway.
 *
 * Everything under /admins is administrator-only — mutating train, route and
 * schedule data is not something an ordinary authenticated user may do.
 */
function getUserContext(req, res, next) {
     const userId = req.headers['x-user-id'];

     if (!userId) {
          return next(
               new UnauthorizedError('User context missing - must come through gateway')
          );
     }

     if (req.headers['x-user-role'] !== 'ADMIN') {
          return next(
               new ForbiddenError('Administrator role required', 'ADMIN_REQUIRED')
          );
     }

     req.user = { id: userId };
     next();
}

module.exports = { getUserContext };