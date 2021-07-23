FROM mcr.microsoft.com/dotnet/framework/runtime
ARG source
WORKDIR /app
COPY ${source:-DataMasker.Main/bin/x64/Release} .
CMD ["DataMasker.Mask.exe"]