import { Request, Response, NextFunction } from "express";
import { plainToInstance, ClassConstructor } from "class-transformer";
import { validate } from "class-validator";

/**
 * @description validates incoming requests based on the provided dto schema
 * @param dtoClass to validate against
 * @example
 * app.post('/users', validateSchema(UserDto), (req, res) => {
 *    res.status(201).json({ message: 'User created', data: req.body });
 * });
 */
export function validateSchema<T extends Object>(
   dtoClass: ClassConstructor<T>,
) {
   return async (req: Request, res: Response, next: NextFunction) => {
      const dtoInstance = plainToInstance(dtoClass, req.body);
      const errors = await validate(dtoInstance);

      if (errors.length > 0) {
         const formattedErrors = errors.map((err) => ({
            property: err.property,
            constraints: err.constraints,
         }));

         return res.status(400).json({ errors: formattedErrors });
      }

      next();
   };
}
