################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CC_SRCS += \
../src/pf/pf.cc \
../src/pf/pftest.cc 

OBJS += \
./src/pf/pf.o \
./src/pf/pftest.o 

CC_DEPS += \
./src/pf/pf.d \
./src/pf/pftest.d 


# Each subdirectory must supply rules for building sources it contributes
src/pf/%.o: ../src/pf/%.cc
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


